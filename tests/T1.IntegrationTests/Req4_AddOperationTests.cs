using Quartz;
using T1.Core.Operations;
using T1.IntegrationTests.Infrastructure;
using T1.Service.Jobs;

namespace T1.IntegrationTests;

/// <summary>4. A scheduled job calls the "add operation" routine every 5 seconds; new rows have state = 0.</summary>
public sealed class Req4_AddOperationTests(PostgresClusterFixture cluster) : IAsyncLifetime
{
    private TestDatabases _db = null!;

    public async ValueTask InitializeAsync()
    {
        _db = await cluster.CreateDatabasesAsync("req4");
        await _db.MigratePrimaryAsync();
        await _db.Primary.ScalarAsync<int>("SELECT t1_ensure_partitions(now(), now() + interval '1 month')");
    }

    public async ValueTask DisposeAsync() => await _db.DisposeAsync();

    [Fact]
    public async Task Add_operation_inserts_a_random_operation_with_state_0()
    {
        var before = DateTime.UtcNow.AddSeconds(-1);
        var operation = await _db.Operations.AddOperationAsync(new NewOperation(), TestContext.Current.CancellationToken);

        Assert.Equal(0, operation.State);
        Assert.InRange(operation.OpDate, before, DateTime.UtcNow.AddSeconds(1));
        Assert.InRange(operation.Amount, 1m, 10_000m);
        Assert.InRange(operation.Message.ClientId, 1, 1000);
        Assert.True(OperationTypes.IsValid(operation.Message.OperationType));
        Assert.Equal(7, operation.OperationGuid.Version); // UUIDv7
        Assert.Equal(operation, await _db.Operations.FindByGuidAsync(operation.OperationGuid, TestContext.Current.CancellationToken));
    }

    [Fact]
    public async Task Add_operation_keeps_given_values()
    {
        var guid = Guid.CreateVersion7();
        var operation = await _db.Operations.AddOperationAsync(
            new NewOperation(Amount: 123.45m, ClientId: 7, OperationType: OperationTypes.Online, AccountNumber: "UA213223130000026007233566001", OperationGuid: guid),
            TestContext.Current.CancellationToken);

        Assert.Equal(0, operation.State);
        Assert.Equal(123.45m, operation.Amount);
        Assert.Equal(new OperationMessage("UA213223130000026007233566001", 7, OperationTypes.Online), operation.Message);
        Assert.Equal(guid, operation.OperationGuid);
    }

    [Fact]
    public void Add_operation_job_fires_every_5_seconds()
    {
        var cron = new JobsOptions().AddOperation.Cron;
        var fireTimes = NextFireTimes(cron, new DateTimeOffset(2026, 11, 12, 10, 0, 1, TimeSpan.Zero), 30);

        Assert.All(fireTimes.Zip(fireTimes.Skip(1)), pair => Assert.Equal(TimeSpan.FromSeconds(5), pair.Second - pair.First));
    }

    internal static List<DateTimeOffset> NextFireTimes(string cron, DateTimeOffset after, int count)
    {
        var expression = CronExpression.Parse(cron).WithTimeZone(TimeZoneInfo.Utc);
        var result = new List<DateTimeOffset>();
        for (var t = expression.GetNextValidTimeAfter(after); t is not null && result.Count < count; t = expression.GetNextValidTimeAfter(t.Value))
        {
            result.Add(t.Value);
        }

        return result;
    }
}
