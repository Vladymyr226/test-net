using System.Net.Sockets;
using Microsoft.Extensions.Logging;
using Npgsql;

namespace T1.Core.Database;

public static class DbConnectivity
{
    /// <summary>Connection-level failures worth a retry (server restart, network, admin shutdown).</summary>
    public static bool IsTransient(Exception exception) =>
        exception is NpgsqlException { IsTransient: true } or SocketException or TimeoutException;

    /// <summary>Retries until the instance accepts queries (containers may still be starting).</summary>
    public static async Task WaitUntilAvailableAsync(NpgsqlDataSource dataSource, string name, ILogger logger, TimeSpan timeout, CancellationToken ct)
    {
        var deadline = DateTime.UtcNow + timeout;
        for (var attempt = 1; ; attempt++)
        {
            try
            {
                await using var connection = await dataSource.OpenConnectionAsync(ct);
                await using var command = new NpgsqlCommand("SELECT 1", connection);
                await command.ExecuteScalarAsync(ct);
                return;
            }
            catch (Exception ex) when (IsTransient(ex) && DateTime.UtcNow < deadline)
            {
                logger.LogWarning("{Name} database is not available yet (attempt {Attempt}): {Error}", name, attempt, ex.Message);
                await Task.Delay(TimeSpan.FromSeconds(Math.Min(attempt, 5)), ct);
            }
        }
    }
}
