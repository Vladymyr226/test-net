using T1.Service.Api;
using T1.Service.Hosting;

var builder = WebApplication.CreateBuilder(args);

builder.Logging.AddSimpleConsole(options =>
{
    options.SingleLine = true;
    options.UseUtcTimestamp = true;
    options.TimestampFormat = "HH:mm:ss.fff ";
});

builder.Services.AddT1Database();
builder.Services.AddT1Jobs(builder.Configuration);

var app = builder.Build();
app.MapT1Api();

try
{
    await app.RunAsync();
    return 0;
}
catch (OperationCanceledException) when (app.Lifetime.ApplicationStopping.IsCancellationRequested)
{
    return 0;   // stopped during start-up
}
catch (Exception ex) when (ex is not HostAbortedException)
{
    // a non-zero exit code lets Docker restart the container
    app.Logger.LogCritical(ex, "The service stopped because of an error");
    return 1;
}

public partial class Program;
