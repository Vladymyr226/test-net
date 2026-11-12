using Microsoft.Extensions.Logging.Abstractions;
using T1.Core;
using T1.Core.Scheduling;
using T1.IntegrationTests.Infrastructure;

namespace T1.IntegrationTests;

/// <summary>Only one service instance runs the scheduled jobs (requirements 4 and 5 must not run twice).</summary>
public sealed class SchedulerLeaderTests(PostgresClusterFixture cluster) : IAsyncLifetime
{
    private TestDatabases _db = null!;

    public async ValueTask InitializeAsync()
    {
        _db = await cluster.CreateDatabasesAsync("lead");
        await _db.MigratePrimaryAsync();
    }

    public async ValueTask DisposeAsync() => await _db.DisposeAsync();

    [Fact]
    public async Task One_instance_leads_and_another_takes_over_when_it_stops()
    {
        await using var first = NewInstance(out var firstSources);
        await using var second = NewInstance(out var secondSources);
        await using var _ = firstSources;
        await using var __ = secondSources;

        Assert.True(await first.TryAcquireOrRenewAsync(TestContext.Current.CancellationToken));
        Assert.False(await second.TryAcquireOrRenewAsync(TestContext.Current.CancellationToken));
        Assert.True(await first.TryAcquireOrRenewAsync(TestContext.Current.CancellationToken));   // renewal keeps it

        await first.DisposeAsync();   // the instance stops and releases the lock

        Assert.True(await Db.EventuallyAsync(() => second.TryAcquireOrRenewAsync(TestContext.Current.CancellationToken), TimeSpan.FromSeconds(5)));
        Assert.False(first.IsLeader);
    }

    [Fact]
    public async Task Leadership_is_lost_with_the_connection()
    {
        await using var leader = NewInstance(out var sources);
        await using var _ = sources;
        Assert.True(await leader.TryAcquireOrRenewAsync(TestContext.Current.CancellationToken));

        await _db.Primary.ExecuteAsync("SELECT pg_terminate_backend(pid) FROM pg_locks WHERE locktype = 'advisory' AND objid = 7310010 AND database = (SELECT oid FROM pg_database WHERE datname = current_database())");

        // notices the dead session and takes the (now free) lock again in the same call: no tick is lost
        Assert.True(await leader.TryAcquireOrRenewAsync(TestContext.Current.CancellationToken));
        Assert.Equal(1L, await _db.Primary.ScalarAsync<long>(
            "SELECT count(*) FROM pg_locks WHERE locktype = 'advisory' AND granted AND objid = 7310010 AND database = (SELECT oid FROM pg_database WHERE datname = current_database())"));
    }

    [Fact]
    public async Task Concurrent_renewals_from_jobs_and_the_background_loop_are_safe()
    {
        await using var leader = NewInstance(out var sources);
        await using var _ = sources;

        var results = await Task.WhenAll(Enumerable.Range(0, 20).Select(_ => leader.TryAcquireOrRenewAsync(TestContext.Current.CancellationToken)));

        Assert.All(results, Assert.True);
    }

    [Fact]
    public void Tick_guid_is_the_same_for_the_same_tick_and_carries_its_time()
    {
        var tick = new DateTimeOffset(2026, 11, 12, 10, 0, 5, TimeSpan.Zero);

        var guid = TickGuid.For("add-operation", tick);

        Assert.Equal(guid, TickGuid.For("add-operation", tick));
        Assert.NotEqual(guid, TickGuid.For("add-operation", tick.AddSeconds(5)));
        Assert.NotEqual(guid, TickGuid.For("other-job", tick));
        Assert.Equal(7, guid.Version);
        Assert.Equal("01a1cf334088", guid.ToString("N")[..12]);   // 48-bit unix ms of the tick
    }

    [Fact]
    public async Task Operation_of_a_tick_is_stored_once_even_if_two_leaders_or_a_retry_insert_it()
    {
        await _db.Primary.ScalarAsync<int>("SELECT t1_ensure_partitions(now(), now())");
        var guid = TickGuid.For("add-operation", DateTimeOffset.UtcNow);

        await _db.Operations.AddOperationAsync(new T1.Core.Operations.NewOperation(OperationGuid: guid), TestContext.Current.CancellationToken);
        var second = await Assert.ThrowsAsync<Npgsql.PostgresException>(() =>
            _db.Operations.AddOperationAsync(new T1.Core.Operations.NewOperation(OperationGuid: guid), TestContext.Current.CancellationToken));

        Assert.Equal(Npgsql.PostgresErrorCodes.UniqueViolation, second.SqlState);   // the job treats it as "already done"
    }

    private LeaderElection NewInstance(out T1DataSources dataSources)
    {
        dataSources = T1DataSources.Create(_db.PrimaryConnectionString, _db.ReplicaConnectionString);
        return new LeaderElection(dataSources, NullLogger<LeaderElection>.Instance);
    }
}
