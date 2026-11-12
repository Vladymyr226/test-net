using Microsoft.Extensions.Logging;
using Npgsql;
using NpgsqlTypes;
using T1.Core.Database;

namespace T1.Core.Scheduling;

/// <summary>
/// Only one service instance runs the scheduled jobs and the replica maintenance: the one holding a
/// session-level advisory lock on the primary. The lock lives as long as its (unpooled) connection.
/// </summary>
public sealed class LeaderElection(T1DataSources db, ILogger<LeaderElection> logger) : IAsyncDisposable
{
    private const long LockKey = 7_310_010;

    private readonly SemaphoreSlim _gate = new(1, 1);
    private NpgsqlConnection? _connection;
    private string? _holder;

    public bool IsLeader { get; private set; }

    /// <summary>
    /// Checks that the session holding the lock is alive, or tries to take the lock (immediately, so a primary
    /// restart costs no tick). Called by every job run as well, which fences a leader that lost its session.
    /// </summary>
    public async Task<bool> TryAcquireOrRenewAsync(CancellationToken ct = default)
    {
        await _gate.WaitAsync(ct);
        try
        {
            if (IsLeader)
            {
                if (await IsAliveAsync(ct))
                {
                    return true;
                }

                IsLeader = false;
                logger.LogWarning("Lost scheduler leadership (the lock session is gone)");
                await CloseAsync();
            }

            return await TryAcquireAsync(ct);
        }
        finally
        {
            _gate.Release();
        }
    }

    public async ValueTask DisposeAsync()
    {
        await _gate.WaitAsync();
        try
        {
            if (IsLeader && _connection is not null)
            {
                try
                {
                    // hand over at once: the server may need a moment to end the session after the close
                    await using var unlock = new NpgsqlCommand("SELECT pg_advisory_unlock(@key)", _connection) { CommandTimeout = 2 };
                    unlock.Parameters.Add(Sql.Param("key", NpgsqlDbType.Bigint, LockKey));
                    await unlock.ExecuteScalarAsync();
                }
                catch (Exception ex) when (IsConnectionFailure(ex))
                {
                    // the session is gone anyway
                }
            }

            IsLeader = false;
            await CloseAsync();
        }
        finally
        {
            _gate.Release();
        }
    }

    private async Task<bool> IsAliveAsync(CancellationToken ct)
    {
        try
        {
            await using var command = new NpgsqlCommand("SELECT 1", _connection) { CommandTimeout = 5 };
            await command.ExecuteScalarAsync(ct);
            return true;
        }
        catch (Exception ex) when (IsConnectionFailure(ex))
        {
            return false;
        }
    }

    private async Task<bool> TryAcquireAsync(CancellationToken ct)
    {
        try
        {
            if (_connection is null)
            {
                _connection = await db.PrimaryUnpooled.OpenConnectionAsync(ct);

                // the server must notice a vanished client within seconds, otherwise its session keeps the lock for hours
                await Sql.ExecuteAsync(_connection,
                    "SET tcp_keepalives_idle = 5; SET tcp_keepalives_interval = 2; SET tcp_keepalives_count = 3; SET tcp_user_timeout = 10000", ct);
            }

            if (await Sql.ScalarAsync<bool>(_connection, "SELECT pg_try_advisory_lock(@key)", ct, Sql.Param("key", NpgsqlDbType.Bigint, LockKey)))
            {
                IsLeader = true;
                _holder = null;
                logger.LogInformation("This instance is the scheduler leader");
                return true;
            }

            var holder = await Sql.ScalarAsync<string?>(_connection, """
                SELECT a.pid || ' from ' || coalesce(host(a.client_addr), 'local') || ' since ' || a.backend_start
                  FROM pg_locks l JOIN pg_stat_activity a ON a.pid = l.pid
                 WHERE l.locktype = 'advisory' AND l.granted AND l.objid = @key
                   AND l.database = (SELECT oid FROM pg_database WHERE datname = current_database())
                """, ct, Sql.Param("key", NpgsqlDbType.Bigint, LockKey));
            if (holder != _holder)
            {
                _holder = holder;
                logger.LogInformation("Scheduler leadership is held by backend {Holder}", holder);
            }

            return false;
        }
        catch (Exception ex) when (IsConnectionFailure(ex))
        {
            logger.LogWarning("Cannot reach the primary for the scheduler lock: {Error}", ex.Message);
            await CloseAsync();
            return false;
        }
    }

    private async Task CloseAsync()
    {
        if (_connection is not null)
        {
            await _connection.DisposeAsync();   // unpooled: ends the session and releases the lock
            _connection = null;
        }
    }

    private static bool IsConnectionFailure(Exception ex) =>
        DbConnectivity.IsTransient(ex) || ex is PostgresException or InvalidOperationException or NpgsqlException;
}
