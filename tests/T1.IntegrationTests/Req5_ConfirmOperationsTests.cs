using T1.Core.Operations;
using T1.IntegrationTests.Infrastructure;
using T1.Service.Jobs;

namespace T1.IntegrationTests;

/// <summary>
/// 5. Every 3 seconds a scheduled job sets state 0 -> 1: for even ids when the number of seconds is even,
/// for odd ids otherwise.
/// </summary>
public sealed class Req5_ConfirmOperationsTests(PostgresClusterFixture cluster) : IAsyncLifetime
{
    private static readonly DateTimeOffset EvenSecond = new(2026, 11, 12, 10, 0, 42, 750, TimeSpan.Zero);
    private static readonly DateTimeOffset OddSecond = new(2026, 11, 12, 10, 0, 45, 10, TimeSpan.Zero);

    private TestDatabases _db = null!;

    public async ValueTask InitializeAsync()
    {
        _db = await cluster.CreateDatabasesAsync("req5");
        await _db.MigratePrimaryAsync();
        await _db.Primary.ScalarAsync<int>("SELECT t1_ensure_partitions(now(), now())");
    }

    public async ValueTask DisposeAsync() => await _db.DisposeAsync();

    [Fact]
    public async Task Even_second_confirms_even_ids_then_odd_second_confirms_odd_ids()
    {
        await _db.ResetPrimaryAsync();
        var ids = new List<long>();
        for (var i = 0; i < 10; i++)
        {
            ids.Add((await _db.Operations.AddOperationAsync(new NewOperation(), TestContext.Current.CancellationToken)).Id);
        }

        var even = await _db.Operations.ConfirmOperationsAsync(EvenSecond, TestContext.Current.CancellationToken);

        Assert.Equal(new ConfirmResult(42, "even", ids.Count(id => id % 2 == 0)), even);
        Assert.Equal(ids.Where(id => id % 2 == 0).Order(), await ConfirmedIds());

        var odd = await _db.Operations.ConfirmOperationsAsync(OddSecond, TestContext.Current.CancellationToken);

        Assert.Equal(new ConfirmResult(45, "odd", ids.Count(id => id % 2 == 1)), odd);
        Assert.Equal(ids.Order(), await ConfirmedIds());
    }

    [Fact]
    public async Task Confirmed_operations_are_not_touched_again()
    {
        await _db.ResetPrimaryAsync();
        await _db.Operations.AddOperationAsync(new NewOperation(), TestContext.Current.CancellationToken);
        await _db.Operations.AddOperationAsync(new NewOperation(), TestContext.Current.CancellationToken);
        await _db.Operations.ConfirmOperationsAsync(EvenSecond, TestContext.Current.CancellationToken);
        await _db.Operations.ConfirmOperationsAsync(OddSecond, TestContext.Current.CancellationToken);

        Assert.Equal(0, (await _db.Operations.ConfirmOperationsAsync(EvenSecond, TestContext.Current.CancellationToken)).ConfirmedCount);
        Assert.Equal(0, (await _db.Operations.ConfirmOperationsAsync(OddSecond, TestContext.Current.CancellationToken)).ConfirmedCount);
        Assert.Equal(0L, await _db.Primary.ScalarAsync<long>("SELECT count(*) FROM t1 WHERE state = 0"));
    }

    [Fact]
    public async Task Without_an_argument_the_current_second_decides()
    {
        var result = (await _db.Primary.RowsAsync("SELECT second_of_minute, parity FROM t1_confirm_operations()"))[0];

        var second = (int)result[0]!;
        Assert.InRange(second, 0, 59);
        Assert.Equal(second % 2 == 0 ? "even" : "odd", result[1]);
    }

    [Fact]
    public async Task Rows_locked_by_another_transaction_are_skipped_instead_of_waited_for()
    {
        await _db.ResetPrimaryAsync();
        var locked = await _db.Operations.AddOperationAsync(new NewOperation(), TestContext.Current.CancellationToken);
        var free = await _db.Operations.AddOperationAsync(new NewOperation(), TestContext.Current.CancellationToken);
        var free2 = await _db.Operations.AddOperationAsync(new NewOperation(), TestContext.Current.CancellationToken);

        await using var holder = await _db.Primary.OpenConnectionAsync(TestContext.Current.CancellationToken);
        await using var transaction = await holder.BeginTransactionAsync(TestContext.Current.CancellationToken);
        await using (var lockRow = new Npgsql.NpgsqlCommand($"SELECT 1 FROM t1 WHERE id = {locked.Id} FOR UPDATE", holder, transaction))
        {
            await lockRow.ExecuteNonQueryAsync(TestContext.Current.CancellationToken);
        }

        var started = DateTime.UtcNow;
        await _db.Operations.ConfirmOperationsAsync(EvenSecond, TestContext.Current.CancellationToken);
        await _db.Operations.ConfirmOperationsAsync(OddSecond, TestContext.Current.CancellationToken);

        Assert.True(DateTime.UtcNow - started < TimeSpan.FromSeconds(2));
        Assert.Equal(new[] { free.Id, free2.Id }.Order(), await ConfirmedIds());   // the locked row waits for its next tick
        await transaction.RollbackAsync(TestContext.Current.CancellationToken);
    }

    [Fact]
    public async Task Parity_also_works_for_negative_ids_and_rejects_infinite_times()
    {
        await _db.ResetPrimaryAsync();
        await _db.Primary.ExecuteAsync(
            "INSERT INTO t1 (id, op_date, amount, operation_guid, message) OVERRIDING SYSTEM VALUE VALUES (-3, now(), 1, gen_random_uuid(), t1_make_message(5, 1, 'online'))");

        await _db.Operations.ConfirmOperationsAsync(OddSecond, TestContext.Current.CancellationToken);

        Assert.Equal(1, await _db.Primary.ScalarAsync<short>("SELECT state FROM t1 WHERE id = -3"));
        await Assert.ThrowsAsync<Npgsql.PostgresException>(() => _db.Primary.ExecuteAsync("SELECT * FROM t1_confirm_operations('infinity')"));
    }

    [Fact]
    public void Confirm_job_fires_every_3_seconds_alternating_even_and_odd_seconds()
    {
        var cron = new JobsOptions().ConfirmOperations.Cron;
        var fireTimes = Req4_AddOperationTests.NextFireTimes(cron, new DateTimeOffset(2026, 11, 12, 10, 0, 1, TimeSpan.Zero), 100); // across minute boundaries

        Assert.All(fireTimes.Zip(fireTimes.Skip(1)), pair =>
        {
            Assert.Equal(TimeSpan.FromSeconds(3), pair.Second - pair.First);
            Assert.NotEqual(pair.First.Second % 2, pair.Second.Second % 2); // so even and odd ids get their turn alternately
        });
    }

    private async Task<IEnumerable<long>> ConfirmedIds() =>
        (await _db.Primary.RowsAsync("SELECT id FROM t1 WHERE state = 1 ORDER BY id")).Select(r => (long)r[0]!);
}
