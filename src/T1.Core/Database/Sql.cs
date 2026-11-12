using Npgsql;
using NpgsqlTypes;

namespace T1.Core.Database;

internal static class Sql
{
    public static NpgsqlParameter Param(string name, NpgsqlDbType type, object? value) =>
        new(name, type) { Value = value ?? DBNull.Value };

    public static async Task<int> ExecuteAsync(NpgsqlConnection connection, string sql, CancellationToken ct, params NpgsqlParameter[] parameters)
    {
        await using var command = new NpgsqlCommand(sql, connection);
        command.Parameters.AddRange(parameters);
        return await command.ExecuteNonQueryAsync(ct);
    }

    public static async Task<T> ScalarAsync<T>(NpgsqlConnection connection, string sql, CancellationToken ct, params NpgsqlParameter[] parameters)
    {
        await using var command = new NpgsqlCommand(sql, connection);
        command.Parameters.AddRange(parameters);
        var result = await command.ExecuteScalarAsync(ct);
        return result is null or DBNull ? default! : (T)result;
    }

    public static async Task<T> ScalarAsync<T>(NpgsqlDataSource dataSource, string sql, CancellationToken ct, params NpgsqlParameter[] parameters)
    {
        await using var connection = await dataSource.OpenConnectionAsync(ct);
        return await ScalarAsync<T>(connection, sql, ct, parameters);
    }

    /// <summary>Session-level advisory lock, released explicitly (a pooled connection would keep it).</summary>
    public static async Task<IAsyncDisposable> AdvisoryLockAsync(NpgsqlConnection connection, long key, CancellationToken ct)
    {
        await ExecuteAsync(connection, "SELECT pg_advisory_lock(@key)", ct, Param("key", NpgsqlDbType.Bigint, key));
        return new AdvisoryLock(connection, key);
    }

    public static bool IsTransient(Exception exception) => DbConnectivity.IsTransient(exception);

    private sealed class AdvisoryLock(NpgsqlConnection connection, long key) : IAsyncDisposable
    {
        public async ValueTask DisposeAsync()
        {
            if (connection.State == System.Data.ConnectionState.Open)
            {
                await ExecuteAsync(connection, "SELECT pg_advisory_unlock(@key)", CancellationToken.None, Param("key", NpgsqlDbType.Bigint, key));
            }
        }
    }
}
