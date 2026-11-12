using Npgsql;
using Quartz;
using T1.Core;
using T1.Core.Operations;
using T1.Core.Scheduling;

namespace T1.Service.Jobs;

/// <summary>Requirement 4: every 5 seconds t1_add_operation() inserts a row with state = 0, exactly once per tick.</summary>
public sealed class AddOperationJob(OperationsRepository operations, LeaderElection leader, T1DataSources db, ILogger<AddOperationJob> logger)
    : LeaderJob(leader, db, logger)
{
    public static readonly JobKey Key = new("add-operation", "t1");

    protected override async Task RunAsync(IJobExecutionContext context, CancellationToken ct)
    {
        var tick = context.ScheduledFireTimeUtc ?? context.FireTimeUtc;
        try
        {
            var operation = await operations.AddOperationAsync(new NewOperation(OperationGuid: TickGuid.For(Key.Name, tick)), ct);
            Logger.LogInformation(
                "add-operation: id={Id} state={State} amount={Amount} client={ClientId} {OperationType} guid={Guid}",
                operation.Id, operation.State, operation.Amount, operation.Message.ClientId, operation.Message.OperationType, operation.OperationGuid);
        }
        catch (PostgresException e) when (e.SqlState == PostgresErrorCodes.UniqueViolation)
        {
            Logger.LogInformation("add-operation: the operation of tick {Tick:HH:mm:ss} already exists", tick);   // a retry or another leader
        }
    }
}
