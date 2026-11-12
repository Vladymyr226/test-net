using Quartz;
using T1.Core;
using T1.Core.Operations;
using T1.Core.Scheduling;

namespace T1.Service.Jobs;

/// <summary>
/// Requirement 5: every 3 seconds state 0 -> 1 for even ids if the current second (database clock) is even,
/// odd ids otherwise. Ticks at 0, 3, ..., 57 alternate the parity; repeating a run is harmless.
/// </summary>
public sealed class ConfirmOperationsJob(OperationsRepository operations, LeaderElection leader, T1DataSources db, ILogger<ConfirmOperationsJob> logger)
    : LeaderJob(leader, db, logger)
{
    public static readonly JobKey Key = new("confirm-operations", "t1");

    protected override async Task RunAsync(IJobExecutionContext context, CancellationToken ct)
    {
        var result = await operations.ConfirmOperationsAsync(null, ct);
        Logger.LogInformation("confirm-operations: second={Second:00} -> {Parity} ids, confirmed {Count}",
            result.SecondOfMinute, result.Parity, result.ConfirmedCount);
    }
}
