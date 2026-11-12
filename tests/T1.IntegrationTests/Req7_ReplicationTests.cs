using Microsoft.Extensions.Logging.Abstractions;
using Npgsql;
using T1.Core;
using T1.Core.Operations;
using T1.Core.Partitions;
using T1.Core.Replication;
using T1.Core.Seeding;
using T1.IntegrationTests.Infrastructure;

namespace T1.IntegrationTests;

/// <summary>7. Replication of T1 to another instance (logical replication).</summary>
public sealed class Req7_ReplicationTests(PostgresClusterFixture cluster) : IAsyncLifetime
{
    private static readonly TimeSpan ReplicationTimeout = TimeSpan.FromSeconds(30);

    private TestDatabases _db = null!;

    public async ValueTask InitializeAsync()
    {
        _db = await cluster.CreateDatabasesAsync("req7");
        await _db.MigratePrimaryAsync();
        await _db.MigrateReplicaAsync();
        await _db.CreateSeeder(new SeedOptions { Rows = 20_000, BatchSize = 5_000 }).SeedIfEmptyAsync(Ct);
        await _db.CreatePartitionManager().SyncAsync(Ct);
        await _db.CreateReplicationConfigurator().EnsureAsync(Ct);
    }

    public async ValueTask DisposeAsync() => await _db.DisposeAsync();

    private static CancellationToken Ct => TestContext.Current.CancellationToken;

    [Fact]
    public async Task Replica_is_a_separate_instance_subscribed_to_the_publication_of_t1()
    {
        Assert.NotEqual(
            await _db.Primary.ScalarAsync<string>("SELECT system_identifier::text FROM pg_control_system()"),
            await _db.Replica.ScalarAsync<string>("SELECT system_identifier::text FROM pg_control_system()"));
        Assert.Equal("t1", await _db.Primary.ScalarAsync<string>("SELECT string_agg(tablename, ',') FROM pg_publication_tables WHERE pubname = 't1_pub'"));
        Assert.True(await _db.Primary.ScalarAsync<bool>("SELECT pubviaroot FROM pg_publication WHERE pubname = 't1_pub'"));
        Assert.True(await _db.Replica.ScalarAsync<bool>("SELECT subenabled FROM pg_subscription WHERE subname = $1", _db.SubscriptionName));
    }

    [Fact]
    public async Task Initial_copy_is_identical_to_the_primary()
    {
        Assert.True(await InSync());
        Assert.True(await _db.Replica.ScalarAsync<long>("SELECT count(*) FROM t1") >= 20_000);
    }

    [Fact]
    public async Task Inserts_updates_and_deletes_are_replicated()
    {
        var operation = await _db.Operations.AddOperationAsync(new NewOperation(Amount: 77.7m), Ct);
        Assert.True(await Db.EventuallyAsync(
            async () => await _db.Replica.ScalarAsync<long>("SELECT count(*) FROM t1 WHERE operation_guid = $1 AND state = 0", operation.OperationGuid) == 1,
            ReplicationTimeout));

        await _db.Primary.ExecuteAsync("UPDATE t1 SET state = 1 WHERE id = $1", operation.Id);
        Assert.True(await Db.EventuallyAsync(
            async () => await _db.Replica.ScalarAsync<short>("SELECT state FROM t1 WHERE operation_guid = $1", operation.OperationGuid) == 1,
            ReplicationTimeout));

        await _db.Primary.ExecuteAsync("DELETE FROM t1 WHERE id = $1", operation.Id);
        Assert.True(await Db.EventuallyAsync(
            async () => await _db.Replica.ScalarAsync<long>("SELECT count(*) FROM t1 WHERE operation_guid = $1", operation.OperationGuid) == 0,
            ReplicationTimeout));
    }

    [Fact]
    public async Task Rows_land_in_the_same_monthly_partitions_on_the_replica()
    {
        var primaryPartitions = await PartitionManager.ListAsync(_db.Primary, Ct);

        Assert.True(await Db.EventuallyAsync(
            async () => primaryPartitions.SequenceEqual(await PartitionManager.ListAsync(_db.Replica, Ct)), ReplicationTimeout));
    }

    [Fact]
    public async Task Partition_created_only_on_the_primary_is_mirrored_and_replication_resumes()
    {
        // e.g. a manual CALL of the generator for an old period: its partitions exist on the primary only
        await _db.Primary.ExecuteAsync("CALL t1_generate_data(p_rows => 500, p_from => '2016-01-01', p_to => '2016-01-10')");
        await _db.Operations.AddOperationAsync(new NewOperation(), Ct);

        var result = await _db.CreatePartitionManager().SyncAsync(Ct);

        Assert.Null(result.ReplicaError);
        Assert.Equal(1, result.CreatedOnReplica);   // 2016-01, despite being more than 10 years back
        Assert.True(await InSync());
    }

    [Fact]
    public async Task Unavailable_replica_does_not_stop_partition_maintenance_of_the_primary()
    {
        var deadReplica = new NpgsqlConnectionStringBuilder(_db.ReplicaConnectionString) { Port = 1, Timeout = 2 }.ConnectionString;
        await using var dataSources = T1DataSources.Create(_db.PrimaryConnectionString, deadReplica);
        var partitions = new PartitionManager(dataSources, new PartitionOptions { MonthsAhead = 6 }, NullLogger<PartitionManager>.Instance);

        var result = await partitions.SyncAsync(Ct);

        Assert.NotNull(result.ReplicaError);
        Assert.Equal(DateTime.UtcNow.AddMonths(6).ToString("yyyy_MM"),
            (await PartitionManager.GetMonthsAsync(_db.Primary, Ct)).Max().ToString("yyyy_MM"));
    }

    [Fact]
    public async Task Lost_subscription_is_recreated_and_the_replica_resynchronized()
    {
        await _db.Replica.ExecuteAsync($"DROP SUBSCRIPTION \"{_db.SubscriptionName}\"");   // the replica keeps its old rows
        await _db.Operations.AddOperationAsync(new NewOperation(), Ct);

        await _db.CreateReplicationConfigurator().EnsureAsync(Ct);

        Assert.True(await InSync());
    }

    [Fact]
    public async Task Subscription_without_a_slot_is_repaired()
    {
        await _db.Replica.ExecuteAsync($"ALTER SUBSCRIPTION \"{_db.SubscriptionName}\" DISABLE");
        await _db.Replica.ExecuteAsync($"ALTER SUBSCRIPTION \"{_db.SubscriptionName}\" SET (slot_name = NONE)");
        await _db.Operations.AddOperationAsync(new NewOperation(), Ct);

        await _db.CreateReplicationConfigurator().EnsureAsync(Ct);

        Assert.True(await InSync());
    }

    [Fact]
    public async Task Changed_credentials_are_applied_to_the_role_and_the_subscription()
    {
        // own role: roles are cluster-wide and other test classes use the default one
        var options = _db.ReplicationOptions;
        options.User = "rep_" + _db.Name;
        foreach (var password in new[] { "first", "rotated 'secret'" })
        {
            options.Password = password;
            await new ReplicationConfigurator(_db.DataSources, options, NullLogger<ReplicationConfigurator>.Instance).EnsureAsync(Ct);

            // the apply worker must reconnect with the new credentials
            await _db.Primary.ExecuteAsync("SELECT pg_terminate_backend(active_pid) FROM pg_replication_slots WHERE slot_name = $1", _db.SubscriptionName);
            await _db.Operations.AddOperationAsync(new NewOperation(), Ct);

            Assert.Contains($"user='{options.User}'", await _db.Replica.ScalarAsync<string>("SELECT subconninfo FROM pg_subscription WHERE subname = $1", _db.SubscriptionName));
            Assert.True(await InSync());
        }
    }

    private async Task<bool> InSync() =>
        await Db.EventuallyAsync(async () => await Fingerprint(_db.Primary) == await Fingerprint(_db.Replica), ReplicationTimeout);

    private static Task<string> Fingerprint(NpgsqlDataSource dataSource) =>
        dataSource.ScalarAsync<string>("""
            SELECT count(*) || ':' || coalesce(md5(string_agg(concat_ws('|', id, op_date, amount, state, operation_guid, message), ',' ORDER BY id)), '')
              FROM t1
            """);
}
