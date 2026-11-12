using Microsoft.Extensions.Logging;
using T1.Core.Database;
using T1.Core.Partitions;
using T1.Core.Replication;
using T1.Core.Seeding;

namespace T1.Core;

/// <summary>
/// Brings the databases to the working state, idempotently. The primary is required for the jobs; the replica
/// is set up separately, so an unavailable replica does not stop the primary.
/// </summary>
public sealed class T1Bootstrapper(
    T1DataSources db,
    DbMigrator migrator,
    DataSeeder seeder,
    PartitionManager partitions,
    ReplicationConfigurator replication,
    ILogger<T1Bootstrapper> logger)
{
    /// <summary>Migrations, test data and partitions of the primary.</summary>
    public async Task InitializePrimaryAsync(CancellationToken ct = default)
    {
        await DbConnectivity.WaitUntilAvailableAsync(db.Primary, "Primary", logger, TimeSpan.FromMinutes(2), ct);
        await migrator.MigrateAsync(db.Primary, DatabaseRole.Primary, ct);
        await seeder.SeedIfEmptyAsync(ct);
        await partitions.EnsurePrimaryAsync(ct);
        logger.LogInformation("Primary database is ready");
    }

    /// <summary>
    /// Migrations and partitions of the replica, then the subscription (initial copy + streaming). Idempotent and
    /// cheap when everything is in place, so it is repeated periodically (a re-created replica is rebuilt).
    /// </summary>
    /// <returns>True when the replica is in sync.</returns>
    public async Task<bool> InitializeReplicaAsync(CancellationToken ct = default)
    {
        await DbConnectivity.WaitUntilAvailableAsync(db.Replica, "Replica", logger, TimeSpan.FromSeconds(10), ct);
        await migrator.MigrateAsync(db.Replica, DatabaseRole.Replica, ct);
        await partitions.SyncReplicaAsync(ct);
        return await replication.EnsureAsync(ct);
    }
}
