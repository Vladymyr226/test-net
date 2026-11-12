using Quartz;
using T1.Core;
using T1.Core.Partitions;
using T1.Core.Scheduling;

namespace T1.Service.Jobs;

/// <summary>Partitions ahead on both instances, and every partition of the primary on the replica.</summary>
public sealed class PartitionMaintenanceJob(PartitionManager partitions, LeaderElection leader, T1DataSources db, ILogger<PartitionMaintenanceJob> logger)
    : LeaderJob(leader, db, logger)
{
    public static readonly JobKey Key = new("partition-maintenance", "t1");

    protected override Task RunAsync(IJobExecutionContext context, CancellationToken ct) => partitions.SyncAsync(ct);
}
