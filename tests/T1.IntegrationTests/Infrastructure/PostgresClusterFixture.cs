using DotNet.Testcontainers.Builders;
using DotNet.Testcontainers.Networks;
using Npgsql;
using Testcontainers.PostgreSql;
using T1.IntegrationTests.Infrastructure;

[assembly: AssemblyFixture(typeof(PostgresClusterFixture))]

namespace T1.IntegrationTests.Infrastructure;

/// <summary>
/// Two independent PostgreSQL 18 instances (as in docker-compose.yml) on a private Docker network, started once
/// per test run. Tests create their own database pairs on them, so test classes run in parallel.
/// </summary>
public sealed class PostgresClusterFixture : IAsyncLifetime
{
    public const string Image = "postgres:18-alpine";

    /// <summary>Host name of the primary inside the Docker network (used by the replica's subscription).</summary>
    public const string PrimaryNetworkAlias = "pg-primary";

    private readonly INetwork _network = new NetworkBuilder().Build();

    public PostgresClusterFixture()
    {
        Primary = new PostgreSqlBuilder(Image)
            .WithNetwork(_network)
            .WithNetworkAliases(PrimaryNetworkAlias)
            .WithCommand("-c", "wal_level=logical", "-c", "max_replication_slots=50", "-c", "max_wal_senders=50", "-c", "max_connections=300")
            .Build();

        Replica = new PostgreSqlBuilder(Image)
            .WithNetwork(_network)
            .WithNetworkAliases("pg-replica")
            .WithCommand("-c", "max_logical_replication_workers=24", "-c", "max_worker_processes=48", "-c", "max_connections=300")
            .Build();
    }

    public PostgreSqlContainer Primary { get; }

    public PostgreSqlContainer Replica { get; }

    public async ValueTask InitializeAsync()
    {
        await _network.CreateAsync();
        await Task.WhenAll(Primary.StartAsync(), Replica.StartAsync());
    }

    public async ValueTask DisposeAsync()
    {
        await Primary.DisposeAsync();
        await Replica.DisposeAsync();
        await _network.DisposeAsync();
    }

    /// <summary>Creates an empty database with the same (unique) name on both instances.</summary>
    public async Task<TestDatabases> CreateDatabasesAsync(string prefix)
    {
        var name = $"t1_{prefix}_{Guid.NewGuid():N}"[..(prefix.Length + 16)];
        foreach (var container in new[] { Primary, Replica })
        {
            await using var connection = new NpgsqlConnection(container.GetConnectionString());
            await connection.OpenAsync();
            await using var command = new NpgsqlCommand($"CREATE DATABASE \"{name}\"", connection);
            await command.ExecuteNonQueryAsync();
        }

        return new TestDatabases(name, WithDatabase(Primary.GetConnectionString(), name), WithDatabase(Replica.GetConnectionString(), name));
    }

    private static string WithDatabase(string connectionString, string database) =>
        new NpgsqlConnectionStringBuilder(connectionString) { Database = database }.ConnectionString;
}
