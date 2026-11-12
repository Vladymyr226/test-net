using System.Globalization;
using System.Text;
using Microsoft.Extensions.Logging;
using Npgsql;
using NpgsqlTypes;
using T1.Core.Database;

namespace T1.Core.Replication;

/// <summary>
/// Requirement 7: logical replication of t1 from the primary to the replica. Idempotent: run it periodically; it
/// also repairs the usual broken states (no slot, slot missing or lost, failing initial copy, changed credentials).
/// </summary>
public sealed class ReplicationConfigurator(T1DataSources db, ReplicationOptions options, ILogger<ReplicationConfigurator> logger)
{
    /// <returns>True when the replica is in sync and streaming.</returns>
    public async Task<bool> EnsureAsync(CancellationToken ct = default)
    {
        await EnsurePublisherAsync(ct);
        await DropOtherSubscriptionsAsync(ct);

        var subscription = await GetSubscriptionAsync(options.SubscriptionName, ct);
        string? reason = null;
        if (subscription is { SlotName: null })
        {
            reason = "it has no replication slot";
        }
        else if (subscription?.SlotName is { } slot && !await SlotUsableAsync(slot, ct))
        {
            reason = $"its slot {slot} is missing or lost on the primary";
        }
        else if (subscription is { Ready: false, SyncErrors: > 0 })
        {
            reason = "the initial copy of t1 keeps failing";
        }

        if (reason is not null)
        {
            logger.LogWarning("Re-creating subscription {Subscription}: {Reason}", options.SubscriptionName, reason);
            await DropSubscriptionAsync(options.SubscriptionName, ct);
            subscription = null;
        }

        if (subscription is null)
        {
            await CreateSubscriptionAsync(ct);
        }
        else
        {
            var conninfo = BuildPublisherConnInfo();
            if (subscription.ConnInfo != conninfo)
            {
                await ExecuteFormattedAsync(db.Replica, "SELECT format('ALTER SUBSCRIPTION %I CONNECTION %L', @sub, @conninfo)", ct, ("conninfo", conninfo));
                logger.LogInformation("Subscription {Subscription}: connection settings updated", options.SubscriptionName);
            }

            if (!subscription.Enabled)
            {
                await ExecuteFormattedAsync(db.Replica, "SELECT format('ALTER SUBSCRIPTION %I ENABLE', @sub)", ct);
                logger.LogInformation("Subscription {Subscription} enabled", options.SubscriptionName);
            }
        }

        return await WaitForInitialSyncAsync(TimeSpan.FromSeconds(options.InitialSyncTimeoutSeconds), ct);
    }

    /// <summary>True once every table of the subscription is synchronized and the apply worker runs.</summary>
    public async Task<bool> WaitForInitialSyncAsync(TimeSpan timeout, CancellationToken ct = default)
    {
        var deadline = DateTime.UtcNow + timeout;
        while (true)
        {
            if (await GetSubscriptionAsync(options.SubscriptionName, ct) is { Ready: true, ApplyWorkerRunning: true })
            {
                return true;
            }

            if (DateTime.UtcNow >= deadline)
            {
                return false;
            }

            await Task.Delay(500, ct);
        }
    }

    private async Task EnsurePublisherAsync(CancellationToken ct)
    {
        await using var connection = await db.Primary.OpenConnectionAsync(ct);

        if (!await Sql.ScalarAsync<bool>(connection, "SELECT EXISTS (SELECT 1 FROM pg_publication WHERE pubname = @pub)", ct,
                Sql.Param("pub", NpgsqlDbType.Text, options.PublicationName)))
        {
            throw new InvalidOperationException($"Publication '{options.PublicationName}' does not exist on the primary.");
        }

        var walLevel = await Sql.ScalarAsync<string>(connection, "SELECT current_setting('wal_level')", ct);
        if (walLevel != "logical")
        {
            throw new InvalidOperationException($"wal_level on the primary is '{walLevel}', logical replication needs 'logical'.");
        }

        // Roles are cluster-wide: concurrent CREATE/ALTER from several databases or instances may collide; retried.
        for (var attempt = 1; ; attempt++)
        {
            try
            {
                var exists = await Sql.ScalarAsync<bool>(connection, "SELECT EXISTS (SELECT 1 FROM pg_roles WHERE rolname = @user)", ct,
                    Sql.Param("user", NpgsqlDbType.Text, options.User));
                await ExecuteFormattedAsync(connection,
                    exists
                        ? "SELECT format('ALTER ROLE %I WITH LOGIN REPLICATION PASSWORD %L', @user, @password)"
                        : "SELECT format('CREATE ROLE %I WITH LOGIN REPLICATION PASSWORD %L', @user, @password)",
                    ct);
                break;
            }
            catch (PostgresException e) when (attempt < 5 && IsConcurrentCatalogUpdate(e))
            {
                await Task.Delay(100 * attempt, ct);
            }
        }

        // the initial copy reads t1 as this role
        await ExecuteFormattedAsync(connection, "SELECT format('GRANT SELECT ON TABLE t1 TO %I', @user)", ct);
    }

    private async Task CreateSubscriptionAsync(CancellationToken ct)
    {
        // the replica was re-created while the primary kept the old slot
        var dropped = await Sql.ScalarAsync<long>(db.Primary,
            "SELECT count(pg_drop_replication_slot(slot_name)) FROM pg_replication_slots WHERE slot_name = @slot AND NOT active", ct,
            Sql.Param("slot", NpgsqlDbType.Text, options.SubscriptionName));
        if (dropped > 0)
        {
            logger.LogWarning("Dropped orphaned replication slot {Slot} on the primary", options.SubscriptionName);
        }

        await using var replica = await db.Replica.OpenConnectionAsync(ct);

        // copy_data copies the whole table: leftovers of an earlier subscription would collide with it
        if (await Sql.ScalarAsync<bool>(replica, "SELECT EXISTS (SELECT 1 FROM t1)", ct))
        {
            logger.LogWarning("Replica t1 has rows but no subscription: truncating it before the initial copy");
            await Sql.ExecuteAsync(replica, "TRUNCATE t1", ct);
        }

        // connects to the primary to create the slot, so it must not run inside a transaction block
        await ExecuteFormattedAsync(replica,
            "SELECT format('CREATE SUBSCRIPTION %I CONNECTION %L PUBLICATION %I WITH (copy_data = true)', @sub, @conninfo, @pub)", ct,
            ("conninfo", BuildPublisherConnInfo()), ("pub", options.PublicationName));

        logger.LogInformation("Created subscription {Subscription} on the replica to {Publication} at {Host}:{Port}",
            options.SubscriptionName, options.PublicationName, options.PublisherHost, options.PublisherPort);
    }

    /// <summary>A renamed subscription must not keep running next to the configured one.</summary>
    private async Task DropOtherSubscriptionsAsync(CancellationToken ct)
    {
        await using var command = db.Replica.CreateCommand("""
            SELECT subname FROM pg_subscription
             WHERE subdbid = (SELECT oid FROM pg_database WHERE datname = current_database())
               AND @pub = ANY (subpublications) AND subname <> @sub
            """);
        command.Parameters.Add(Sql.Param("pub", NpgsqlDbType.Text, options.PublicationName));
        command.Parameters.Add(Sql.Param("sub", NpgsqlDbType.Text, options.SubscriptionName));
        var others = new List<string>();
        await using (var reader = await command.ExecuteReaderAsync(ct))
        {
            while (await reader.ReadAsync(ct))
            {
                others.Add(reader.GetString(0));
            }
        }

        foreach (var name in others)
        {
            logger.LogWarning("Dropping subscription {Subscription}: replaced by {Configured}", name, options.SubscriptionName);
            await DropSubscriptionAsync(name, ct);
        }
    }

    private async Task DropSubscriptionAsync(string name, CancellationToken ct)
    {
        // Detach the slot first, so DROP does not need the primary for it (one transaction: no half state).
        await using var replica = await db.Replica.OpenConnectionAsync(ct);
        await using var transaction = await replica.BeginTransactionAsync(ct);
        await ExecuteFormattedAsync(replica, "SELECT format('ALTER SUBSCRIPTION %I DISABLE', @name)", ct, ("name", name));
        await ExecuteFormattedAsync(replica, "SELECT format('ALTER SUBSCRIPTION %I SET (slot_name = NONE)', @name)", ct, ("name", name));
        await ExecuteFormattedAsync(replica, "SELECT format('DROP SUBSCRIPTION %I', @name)", ct, ("name", name));
        await transaction.CommitAsync(ct);

        // the slot (if any) is now orphaned on the primary
        await Sql.ScalarAsync<long>(db.Primary,
            "SELECT count(pg_drop_replication_slot(slot_name)) FROM pg_replication_slots WHERE slot_name = @slot AND NOT active", ct,
            Sql.Param("slot", NpgsqlDbType.Text, name));
    }

    private async Task<SubscriptionState?> GetSubscriptionAsync(string name, CancellationToken ct)
    {
        await using var command = db.Replica.CreateCommand("""
            SELECT s.subenabled, s.subslotname, s.subconninfo,
                   coalesce(bool_and(r.srsubstate = 'r'), false),
                   coalesce(max(st.sync_error_count), 0),
                   EXISTS (SELECT 1 FROM pg_stat_subscription w WHERE w.subid = s.oid AND w.worker_type = 'apply' AND w.pid IS NOT NULL)
              FROM pg_subscription s
              LEFT JOIN pg_subscription_rel r ON r.srsubid = s.oid
              LEFT JOIN pg_stat_subscription_stats st ON st.subid = s.oid
             WHERE s.subname = @sub
               AND s.subdbid = (SELECT oid FROM pg_database WHERE datname = current_database())
             GROUP BY s.oid, s.subenabled, s.subslotname, s.subconninfo
            """);
        command.Parameters.Add(Sql.Param("sub", NpgsqlDbType.Text, name));
        await using var reader = await command.ExecuteReaderAsync(ct);
        if (!await reader.ReadAsync(ct))
        {
            return null;
        }

        return new SubscriptionState(
            reader.GetBoolean(0),
            reader.IsDBNull(1) ? null : reader.GetString(1),
            reader.GetString(2),
            reader.GetBoolean(3),
            reader.GetInt64(4),
            reader.GetBoolean(5));
    }

    /// <summary>"lost": the slot exceeded max_slot_wal_keep_size and can no longer stream.</summary>
    private Task<bool> SlotUsableAsync(string slotName, CancellationToken ct) =>
        Sql.ScalarAsync<bool>(db.Primary,
            "SELECT EXISTS (SELECT 1 FROM pg_replication_slots WHERE slot_name = @slot AND wal_status IS DISTINCT FROM 'lost')", ct,
            Sql.Param("slot", NpgsqlDbType.Text, slotName));

    private async Task ExecuteFormattedAsync(NpgsqlDataSource dataSource, string formatQuery, CancellationToken ct, params (string Name, string Value)[] extra)
    {
        await using var connection = await dataSource.OpenConnectionAsync(ct);
        await ExecuteFormattedAsync(connection, formatQuery, ct, extra);
    }

    /// <summary>DDL cannot take bind parameters: format() on the server quotes identifiers (%I) and literals (%L).</summary>
    private async Task ExecuteFormattedAsync(NpgsqlConnection connection, string formatQuery, CancellationToken ct, params (string Name, string Value)[] extra)
    {
        await using var command = new NpgsqlCommand(formatQuery, connection);
        foreach (var (name, value) in new[] { ("user", options.User), ("password", options.Password), ("sub", options.SubscriptionName) }.Concat(extra))
        {
            if (formatQuery.Contains("@" + name, StringComparison.Ordinal))
            {
                command.Parameters.Add(Sql.Param(name, NpgsqlDbType.Text, value));
            }
        }

        var ddl = (string)(await command.ExecuteScalarAsync(ct))!;
        await Sql.ExecuteAsync(connection, ddl, ct);
    }

    /// <summary>libpq connection string the replica uses to reach the primary.</summary>
    internal string BuildPublisherConnInfo()
    {
        var database = string.IsNullOrEmpty(options.PublisherDatabase)
            ? new NpgsqlConnectionStringBuilder(db.Primary.ConnectionString).Database ?? "postgres"
            : options.PublisherDatabase;

        var conninfo = new StringBuilder();
        foreach (var (key, value) in new[]
                 {
                     ("host", options.PublisherHost),
                     ("port", options.PublisherPort.ToString(CultureInfo.InvariantCulture)),
                     ("dbname", database),
                     ("user", options.User),
                     ("password", options.Password),
                 })
        {
            // libpq quoting: single quotes, backslash-escaped
            conninfo.Append(conninfo.Length > 0 ? " " : "").Append(key).Append("='")
                .Append(value.Replace(@"\", @"\\", StringComparison.Ordinal).Replace("'", @"\'", StringComparison.Ordinal)).Append('\'');
        }

        return conninfo.ToString();
    }

    private static bool IsConcurrentCatalogUpdate(PostgresException e) =>
        e.SqlState is PostgresErrorCodes.DuplicateObject or PostgresErrorCodes.UniqueViolation
        || (e.SqlState == PostgresErrorCodes.InternalError && e.MessageText.Contains("concurrently", StringComparison.Ordinal));

    private sealed record SubscriptionState(bool Enabled, string? SlotName, string ConnInfo, bool Ready, long SyncErrors, bool ApplyWorkerRunning);
}
