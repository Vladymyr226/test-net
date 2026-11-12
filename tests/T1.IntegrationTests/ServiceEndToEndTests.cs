using System.Net;
using System.Net.Http.Json;
using System.Text.Json;
using Microsoft.AspNetCore.Hosting;
using Microsoft.AspNetCore.Mvc.Testing;
using Microsoft.Extensions.DependencyInjection;
using Quartz;
using T1.IntegrationTests.Infrastructure;
using T1.Service.Jobs;

namespace T1.IntegrationTests;

/// <summary>
/// The real service (Program.cs) against two fresh databases: bootstrap (migrations, seed, partitions,
/// replication), Quartz jobs on their real schedules, the materialized view, replication and the HTTP API.
/// </summary>
public sealed class ServiceEndToEndTests(ServiceEndToEndTests.ServiceHost service) : IClassFixture<ServiceEndToEndTests.ServiceHost>
{
    private const int SeedRows = 5_000;

    private TestDatabases Databases => service.Databases;

    private HttpClient Client => service.Client;

    private WebApplicationFactory<Program> Factory => service.Factory;

    private CancellationToken Ct => TestContext.Current.CancellationToken;

    [Fact]
    public async Task Service_bootstraps_both_databases()
    {
        Assert.True(await Databases.Primary.ScalarAsync<long>("SELECT count(*) FROM t1") >= SeedRows);
        Assert.Equal(5L, await Databases.Primary.ScalarAsync<long>("SELECT count(*) FROM schema_migrations"));

        // the replica is set up in the background by the scheduler leader
        Assert.True(await Db.EventuallyAsync(async () =>
        {
            var health = await Client.GetFromJsonAsync<JsonElement>("/health", Ct);
            return health.GetProperty("schedulerLeader").GetBoolean() && health.GetProperty("replicaSetup").GetString() == "in-sync"
                   && health.GetProperty("replica").GetString() == "streaming";
        }, TimeSpan.FromSeconds(60)));
        Assert.Equal(1L, await Databases.Replica.ScalarAsync<long>("SELECT count(*) FROM schema_migrations"));
        Assert.True(await Db.EventuallyAsync(async () => (await Replication()).GetProperty("dataInSync").GetBoolean(), TimeSpan.FromSeconds(30)));
    }

    [Fact]
    public async Task Jobs_are_scheduled_as_required()
    {
        var scheduler = await Factory.Services.GetRequiredService<ISchedulerFactory>().GetScheduler(Ct);

        Assert.Equal(SchedulerStatus.Running, await scheduler.GetStatus(Ct));
        Assert.Equal("0/5 * * * * ?", await CronOf(scheduler, AddOperationJob.Key));
        Assert.Equal("0/3 * * * * ?", await CronOf(scheduler, ConfirmOperationsJob.Key));
    }

    [Fact]
    public async Task Every_5_seconds_a_pending_operation_is_added_and_soon_confirmed_by_id_parity()
    {
        // The seed fills ids 1..SeedRows. The guid of a job row carries its tick (TickGuid), which is exact even when
        // the machine is busy; rows added through the API by other tests have client ids above 1000 and are excluded.
        const string jobTicks = """
            SELECT extract(epoch FROM uuid_extract_timestamp(operation_guid))::numeric
              FROM t1 WHERE id > $1 AND (message ->> 'client_id')::bigint <= 1000 ORDER BY id
            """;

        Assert.True(await Db.EventuallyAsync(async () => (await Databases.Primary.RowsAsync(jobTicks, (long)SeedRows)).Count >= 4, TimeSpan.FromSeconds(40)));

        var ticks = (await Databases.Primary.RowsAsync(jobTicks, (long)SeedRows)).Select(r => (decimal)r[0]!).ToList();
        Assert.All(ticks, tick => Assert.Equal(0m, tick % 5));                                      // on the 0/5 cron grid
        Assert.Equal(ticks.Count, ticks.Distinct().Count());                                        // one row per tick
        Assert.All(ticks.Zip(ticks.Skip(1)), pair => Assert.True(pair.Second > pair.First));
        Assert.True(ticks.Zip(ticks.Skip(1)).Count(pair => pair.Second - pair.First == 5m) >= ticks.Count / 2);   // mostly every tick

        // The 3-second job confirms every operation within two ticks (one of each parity), seeded ones included.
        Assert.True(await Db.EventuallyAsync(
            async () => await Databases.Primary.ScalarAsync<long>("SELECT count(*) FROM t1 WHERE state = 0 AND op_date < now() - interval '10 seconds'") == 0,
            TimeSpan.FromSeconds(20)));
    }

    [Fact]
    public async Task Totals_view_follows_confirmations_and_replica_follows_the_primary()
    {
        await Task.Delay(TimeSpan.FromSeconds(7), Ct); // let both jobs run a few times

        Assert.Equal(0L, await Databases.Primary.ScalarAsync<long>("""
            WITH live AS (
                SELECT (message ->> 'client_id')::bigint, message ->> 'operation_type', sum(amount), count(*)
                  FROM t1 WHERE state = 1 GROUP BY 1, 2)
            SELECT count(*) FROM ((SELECT * FROM live EXCEPT SELECT * FROM t1_client_totals)
                                  UNION ALL (SELECT * FROM t1_client_totals EXCEPT SELECT * FROM live)) d
            """));

        var totals = await Client.GetFromJsonAsync<JsonElement>("/api/client-totals?limit=5", Ct);
        Assert.Equal(5, totals.GetProperty("rows").GetArrayLength());

        Assert.True(await Db.EventuallyAsync(async () => (await Replication()).GetProperty("dataInSync").GetBoolean(), TimeSpan.FromSeconds(30)));
    }

    [Fact]
    public async Task Api_adds_operations_and_rejects_duplicate_guids()
    {
        var guid = Guid.CreateVersion7();
        var created = await Client.PostAsJsonAsync("/api/operations", new { amount = 10.5m, clientId = 5_000_001, operationType = "online", operationGuid = guid }, Ct);
        Assert.Equal(HttpStatusCode.Created, created.StatusCode);
        var body = await created.Content.ReadFromJsonAsync<JsonElement>(Ct);
        Assert.Equal(0, body.GetProperty("state").GetInt32());
        Assert.Equal(guid, body.GetProperty("operationGuid").GetGuid());

        var found = await Client.GetFromJsonAsync<JsonElement>($"/api/operations/{guid}", Ct); // state may already be 1: the job runs every 3 s
        Assert.Equal(body.GetProperty("id").GetInt64(), found.GetProperty("id").GetInt64());

        var duplicate = await Client.PostAsJsonAsync("/api/operations", new { amount = 1m, clientId = 5_000_001, operationGuid = guid }, Ct);
        Assert.Equal(HttpStatusCode.Conflict, duplicate.StatusCode);

        var invalid = await Client.PostAsJsonAsync("/api/operations", new { operationType = "cash" }, Ct);
        Assert.Equal(HttpStatusCode.BadRequest, invalid.StatusCode);
    }

    [Fact]
    public async Task Replica_recreated_while_the_service_runs_is_rebuilt()
    {
        Assert.True(await Db.EventuallyAsync(async () => (await Replication()).GetProperty("dataInSync").GetBoolean(), TimeSpan.FromSeconds(60)));

        // "the replica volume was wiped": its database (and subscription) are gone, the primary keeps an orphaned slot
        await using (var admin = Npgsql.NpgsqlDataSource.Create(service.ReplicaAdminConnectionString))
        {
            foreach (var sql in new[] { "DISABLE", "SET (slot_name = NONE)" })
            {
                await admin.ExecuteAsync($"ALTER SUBSCRIPTION \"{Databases.SubscriptionName}\" {sql}");
            }

            await admin.ExecuteAsync($"DROP SUBSCRIPTION \"{Databases.SubscriptionName}\"");
        }

        await using (var server = Npgsql.NpgsqlDataSource.Create(new Npgsql.NpgsqlConnectionStringBuilder(service.ReplicaAdminConnectionString) { Database = "postgres" }.ConnectionString))
        {
            await server.ExecuteAsync($"DROP DATABASE \"{Databases.Name}\" WITH (FORCE)");
            await server.ExecuteAsync($"CREATE DATABASE \"{Databases.Name}\"");
        }

        Databases.Replica.Clear();
        Assert.True(await Db.EventuallyAsync(async () =>
        {
            try
            {
                return (await Replication()).GetProperty("dataInSync").GetBoolean();
            }
            catch (HttpRequestException)
            {
                return false;
            }
        }, TimeSpan.FromSeconds(90)));
    }

    private async Task<JsonElement> Replication() => await Client.GetFromJsonAsync<JsonElement>("/api/replication", Ct);

    private async Task<string?> CronOf(IScheduler scheduler, JobKey job) =>
        (await scheduler.GetTriggersOfJob(job, Ct)).OfType<ICronTrigger>().SingleOrDefault()?.CronExpressionString;

    /// <summary>One running service (and database pair) shared by the tests of this class.</summary>
    public sealed class ServiceHost(PostgresClusterFixture cluster) : IAsyncLifetime
    {
        public TestDatabases Databases { get; private set; } = null!;

        public WebApplicationFactory<Program> Factory { get; private set; } = null!;

        public HttpClient Client { get; private set; } = null!;

        public string ReplicaAdminConnectionString => Databases.ReplicaConnectionString;

        public async ValueTask InitializeAsync()
        {
            var db = Databases = await cluster.CreateDatabasesAsync("e2e");
            Factory = new WebApplicationFactory<Program>().WithWebHostBuilder(host =>
            {
                host.UseSetting("ConnectionStrings:Primary", db.PrimaryConnectionString);
                host.UseSetting("ConnectionStrings:Replica", db.ReplicaConnectionString);
                host.UseSetting("Seed:Rows", SeedRows.ToString(System.Globalization.CultureInfo.InvariantCulture));
                host.UseSetting("Seed:BatchSize", "1000");
                host.UseSetting("Replication:SubscriptionName", db.SubscriptionName);
                host.UseSetting("Replication:PublisherHost", PostgresClusterFixture.PrimaryNetworkAlias);
                host.UseSetting("Replication:PublisherPort", "5432");
                host.UseSetting("Replication:PublisherDatabase", db.Name);
                host.UseSetting("Replication:CheckIntervalSeconds", "5");
            });

            Client = Factory.CreateClient(); // starts the host: bootstrap, then Quartz
        }

        public async ValueTask DisposeAsync()
        {
            Client.Dispose();
            await Factory.DisposeAsync(); // stops Quartz before the subscription is dropped
            await Databases.DisposeAsync();
        }
    }
}
