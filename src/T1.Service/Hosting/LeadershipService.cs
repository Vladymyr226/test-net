using T1.Core.Scheduling;

namespace T1.Service.Hosting;

/// <summary>Keeps trying to become (and stay) the scheduler leader.</summary>
public sealed class LeadershipService(LeaderElection leader) : BackgroundService
{
    protected override async Task ExecuteAsync(CancellationToken stoppingToken)
    {
        while (!stoppingToken.IsCancellationRequested)
        {
            await leader.TryAcquireOrRenewAsync(stoppingToken);
            await Task.Delay(TimeSpan.FromSeconds(2), stoppingToken);
        }
    }

    public override async Task StopAsync(CancellationToken cancellationToken)
    {
        await base.StopAsync(cancellationToken);
        await leader.DisposeAsync();
    }
}
