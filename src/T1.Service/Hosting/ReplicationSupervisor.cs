using T1.Core;
using T1.Core.Replication;
using T1.Core.Scheduling;

namespace T1.Service.Hosting;

public sealed class ReplicaStatus
{
    /// <summary>pending | in-sync | syncing | error</summary>
    public string State { get; set; } = "pending";

    public string? Error { get; set; }
}

/// <summary>
/// Leader only: sets up the replica in the background (the primary and the jobs do not wait for it) and repeats the
/// idempotent setup periodically, which rebuilds a re-created replica and repairs a broken subscription.
/// </summary>
public sealed class ReplicationSupervisor(
    T1Bootstrapper bootstrapper,
    LeaderElection leader,
    ReplicationOptions options,
    ReplicaStatus status,
    ILogger<ReplicationSupervisor> logger) : BackgroundService
{
    protected override async Task ExecuteAsync(CancellationToken stoppingToken)
    {
        var failures = 0;
        while (!stoppingToken.IsCancellationRequested)
        {
            if (await leader.TryAcquireOrRenewAsync(stoppingToken))
            {
                try
                {
                    var state = await bootstrapper.InitializeReplicaAsync(stoppingToken) ? "in-sync" : "syncing";
                    if (state != status.State)
                    {
                        logger.LogInformation("Replica: {State}", state);
                    }

                    status.State = state;
                    status.Error = null;
                    failures = 0;
                }
                catch (Exception ex) when (ex is not OperationCanceledException)
                {
                    if (ex.Message != status.Error)
                    {
                        logger.LogError(ex, "Replica setup failed, will retry");
                    }

                    status.State = "error";
                    status.Error = ex.Message;
                    failures++;
                }
            }

            // retry failures with a growing pause (5 s .. the regular interval)
            var interval = Math.Max(options.CheckIntervalSeconds, 5);
            var delay = failures == 0 ? interval : Math.Min(5 << Math.Min(failures - 1, 4), interval);
            await Task.Delay(TimeSpan.FromSeconds(delay), stoppingToken);
        }
    }
}
