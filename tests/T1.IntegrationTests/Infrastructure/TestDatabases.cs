using Microsoft.Extensions.Logging.Abstractions;
using Npgsql;
using T1.Core;
using T1.Core.Database;
using T1.Core.Operations;
using T1.Core.Partitions;
using T1.Core.Replication;
using T1.Core.Seeding;

namespace T1.IntegrationTests.Infrastructure;

/// <summary>A database pair (same name on the primary and on the replica instance) owned by one test class.</summary>
public sealed class TestDatabases : IAsyncDisposable
{
    public TestDatabases(string name, string primaryConnectionString, string replicaConnectionString)
    {
        Name = name;
        PrimaryConnectionString = primaryConnectionString;
        ReplicaConnectionString = replicaConnectionString;
        DataSources = T1DataSources.Create(primaryConnectionString, replicaConnectionString);
        Operations = new OperationsRepository(DataSources);
    }

    public string Name { get; }

    public string PrimaryConnectionString { get; }

    public string ReplicaConnectionString { get; }

    public T1DataSources DataSources { get; }

    public NpgsqlDataSource Primary => DataSources.Primary;

    public NpgsqlDataSource Replica => DataSources.Replica;

    public OperationsRepository Operations { get; }

    /// <summary>Unique subscription (= slot) name: slots are cluster-wide on the primary.</summary>
    public string SubscriptionName => "sub_" + Name;

    public ReplicationOptions ReplicationOptions => new()
    {
        SubscriptionName = SubscriptionName,
        PublisherHost = PostgresClusterFixture.PrimaryNetworkAlias,
        PublisherPort = 5432,
        PublisherDatabase = Name,
        InitialSyncTimeoutSeconds = 60,
    };

    public async Task MigratePrimaryAsync() =>
        await new DbMigrator(NullLogger<DbMigrator>.Instance).MigrateAsync(Primary, DatabaseRole.Primary, TestContext.Current.CancellationToken);

    public async Task MigrateReplicaAsync() =>
        await new DbMigrator(NullLogger<DbMigrator>.Instance).MigrateAsync(Replica, DatabaseRole.Replica, TestContext.Current.CancellationToken);

    public DataSeeder CreateSeeder(SeedOptions options) => new(DataSources, options, NullLogger<DataSeeder>.Instance);

    public PartitionManager CreatePartitionManager() => new(DataSources, new PartitionOptions(), NullLogger<PartitionManager>.Instance);

    public ReplicationConfigurator CreateReplicationConfigurator() => new(DataSources, ReplicationOptions, NullLogger<ReplicationConfigurator>.Instance);

    /// <summary>Removes all operations (TRUNCATE also clears the guid registry) and refreshes the totals view.</summary>
    public async Task ResetPrimaryAsync()
    {
        await Primary.ExecuteAsync("TRUNCATE t1");
        await Primary.ExecuteAsync("SELECT t1_refresh_client_totals()");
    }

    /// <summary>A login role with plain DML rights on t1 (roles are cluster-wide, so the name is unique).</summary>
    public async Task<NpgsqlDataSource> CreateDmlRoleAsync()
    {
        var role = "r_" + Name;
        await Primary.ExecuteAsync($"CREATE ROLE \"{role}\" LOGIN PASSWORD 'pw'");
        await Primary.ExecuteAsync($"GRANT SELECT, INSERT, UPDATE, DELETE ON t1 TO \"{role}\"");
        var builder = new NpgsqlConnectionStringBuilder(PrimaryConnectionString) { Username = role, Password = "pw" };
        return NpgsqlDataSource.Create(builder.ConnectionString);
    }

    public async ValueTask DisposeAsync()
    {
        try
        {
            // A subscription keeps an apply worker on the replica and a slot on the primary.
            if (await Replica.ScalarAsync<bool>("SELECT EXISTS (SELECT 1 FROM pg_subscription WHERE subname = $1)", SubscriptionName))
            {
                await Replica.ExecuteAsync($"DROP SUBSCRIPTION \"{SubscriptionName}\"");
            }
        }
        catch (NpgsqlException)
        {
            // best effort: the containers are thrown away at the end of the run anyway
        }

        await DataSources.DisposeAsync();
    }
}
