using Quartz;
using T1.Core;
using T1.Core.Database;
using T1.Core.Scheduling;

namespace T1.Service.Jobs;

/// <summary>Runs on the scheduler leader only; retries once after a transient database error (e.g. a restart).</summary>
public abstract class LeaderJob : IJob
{
    private readonly LeaderElection _leader;
    private readonly T1DataSources _db;

    protected LeaderJob(LeaderElection leader, T1DataSources db, ILogger logger)
    {
        _leader = leader;
        _db = db;
        Logger = logger;
    }

    protected ILogger Logger { get; }

    public async ValueTask Execute(IJobExecutionContext context, CancellationToken cancellationToken)
    {
        for (var attempt = 1; ; attempt++)
        {
            try
            {
                if (!await _leader.TryAcquireOrRenewAsync(cancellationToken))
                {
                    Logger.LogDebug("{Job}: skipped, another instance is the scheduler leader", context.JobDetail.Key.Name);
                    return;
                }

                await RunAsync(context, cancellationToken);
                return;
            }
            catch (Exception ex) when (attempt == 1 && DbConnectivity.IsTransient(ex))
            {
                _db.Primary.Clear();   // drop pooled connections broken by the restart
            }
            catch (Exception ex) when (ex is not OperationCanceledException)
            {
                Logger.LogError(ex, "{Job} failed", context.JobDetail.Key.Name);
                return;
            }
        }
    }

    protected abstract Task RunAsync(IJobExecutionContext context, CancellationToken ct);
}
