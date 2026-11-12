using Microsoft.Extensions.Logging;
using Npgsql;
using NpgsqlTypes;
using T1.Core.Database;

namespace T1.Core.Partitions;

public sealed class PartitionOptions
{
    public int MonthsAhead { get; set; } = 2;
}

public sealed record PartitionSyncResult(int CreatedOnReplica, int CreatedOnPrimary, string? ReplicaError);

public sealed record PartitionInfo(string Name, string Bounds, long Rows);

/// <summary>
/// Keeps monthly partitions of t1 on both instances: MonthsAhead months in advance, and every partition of the
/// primary also on the replica (DDL is not replicated, and a row without a partition stops the subscription).
/// The replica always goes first.
/// </summary>
public sealed class PartitionManager(T1DataSources db, PartitionOptions options, ILogger<PartitionManager> logger)
{
    private string? _lastReplicaError;

    /// <summary>Replica (best effort: an unavailable replica must not stop the primary), then the primary.</summary>
    public async Task<PartitionSyncResult> SyncAsync(CancellationToken ct = default)
    {
        var primaryMonths = await GetMonthsAsync(db.Primary, ct);
        var onReplica = 0;
        string? replicaError = null;
        try
        {
            onReplica = await SyncReplicaAsync(primaryMonths, ct);
        }
        catch (PostgresException ex) when (ex.SqlState is PostgresErrorCodes.UndefinedFunction or PostgresErrorCodes.UndefinedTable)
        {
            replicaError = "the replica is not migrated yet";
        }
        catch (Exception ex) when (Sql.IsTransient(ex) || ex is PostgresException)
        {
            replicaError = ex.Message;
            if (replicaError != _lastReplicaError)
            {
                logger.LogWarning("Partitions of the replica were not synchronized: {Error}", ex.Message);
            }
        }

        _lastReplicaError = replicaError;
        var onPrimary = await EnsurePrimaryAsync(ct);
        if (onReplica + onPrimary > 0)
        {
            logger.LogInformation("Created partitions of t1: {Replica} on the replica, {Primary} on the primary", onReplica, onPrimary);
        }

        return new PartitionSyncResult(onReplica, onPrimary, replicaError);
    }

    public Task<int> EnsurePrimaryAsync(CancellationToken ct = default)
    {
        var (from, to) = Window();
        return EnsureAsync(db.Primary, from, to, ct);
    }

    /// <summary>Creates on the replica the partitions of the look-ahead window and every partition the primary has.</summary>
    public async Task<int> SyncReplicaAsync(CancellationToken ct = default) =>
        await SyncReplicaAsync(await GetMonthsAsync(db.Primary, ct), ct);

    private async Task<int> SyncReplicaAsync(IReadOnlyList<DateTime> primaryMonths, CancellationToken ct)
    {
        var (from, to) = Window();
        var created = await EnsureAsync(db.Replica, from, to, ct);

        var onReplica = (await GetMonthsAsync(db.Replica, ct)).ToHashSet();
        foreach (var month in primaryMonths.Where(m => !onReplica.Contains(m)))
        {
            created += await EnsureAsync(db.Replica, month, month, ct);
        }

        return created;
    }

    public static Task<int> EnsureAsync(NpgsqlDataSource dataSource, DateTime fromUtc, DateTime toUtc, CancellationToken ct) =>
        Sql.ScalarAsync<int>(dataSource, "SELECT t1_ensure_partitions(@from, @to)", ct,
            Sql.Param("from", NpgsqlDbType.TimestampTz, fromUtc),
            Sql.Param("to", NpgsqlDbType.TimestampTz, toUtc));

    public static async Task<IReadOnlyList<DateTime>> GetMonthsAsync(NpgsqlDataSource dataSource, CancellationToken ct)
    {
        await using var command = dataSource.CreateCommand("SELECT m FROM t1_partition_months() m");
        await using var reader = await command.ExecuteReaderAsync(ct);
        var months = new List<DateTime>();
        while (await reader.ReadAsync(ct))
        {
            months.Add(reader.GetFieldValue<DateTime>(0));
        }

        return months;
    }

    public static async Task<IReadOnlyList<PartitionInfo>> ListAsync(NpgsqlDataSource dataSource, CancellationToken ct)
    {
        await using var command = dataSource.CreateCommand("""
            WITH counts AS (SELECT tableoid, count(*) AS rows FROM t1 GROUP BY tableoid)
            SELECT c.relname, pg_get_expr(c.relpartbound, c.oid), coalesce(counts.rows, 0)
              FROM pg_inherits i
              JOIN pg_class c ON c.oid = i.inhrelid
              LEFT JOIN counts ON counts.tableoid = c.oid
             WHERE i.inhparent = 't1'::regclass
             ORDER BY c.relname
            """);
        await using var reader = await command.ExecuteReaderAsync(ct);
        var result = new List<PartitionInfo>();
        while (await reader.ReadAsync(ct))
        {
            result.Add(new PartitionInfo(reader.GetString(0), reader.GetString(1), reader.GetInt64(2)));
        }

        return result;
    }

    private (DateTime From, DateTime To) Window()
    {
        var now = DateTime.UtcNow;
        return (now, now.AddMonths(Math.Max(options.MonthsAhead, 0)));
    }
}
