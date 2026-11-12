using T1.Core;

namespace T1.Service.Hosting;

/// <summary>Prepares the primary before anything else starts (registered before Quartz); a failure stops the host.</summary>
public sealed class DatabaseInitializer(T1Bootstrapper bootstrapper) : IHostedService
{
    public Task StartAsync(CancellationToken cancellationToken) => bootstrapper.InitializePrimaryAsync(cancellationToken);

    public Task StopAsync(CancellationToken cancellationToken) => Task.CompletedTask;
}
