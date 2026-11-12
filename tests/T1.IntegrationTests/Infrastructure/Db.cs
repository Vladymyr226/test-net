using System.Globalization;
using Npgsql;

namespace T1.IntegrationTests.Infrastructure;

/// <summary>Terse SQL helpers for assertions; parameters are positional ($1, $2, ...).</summary>
public static class Db
{
    public static async Task<T> ScalarAsync<T>(this NpgsqlDataSource dataSource, string sql, params object?[] args)
    {
        await using var command = Command(dataSource, sql, args);
        var value = await command.ExecuteScalarAsync(TestContext.Current.CancellationToken);
        return Convert<T>(value);
    }

    public static async Task<int> ExecuteAsync(this NpgsqlDataSource dataSource, string sql, params object?[] args)
    {
        await using var command = Command(dataSource, sql, args);
        return await command.ExecuteNonQueryAsync(TestContext.Current.CancellationToken);
    }

    public static async Task<List<object?[]>> RowsAsync(this NpgsqlDataSource dataSource, string sql, params object?[] args)
    {
        await using var command = Command(dataSource, sql, args);
        await using var reader = await command.ExecuteReaderAsync(TestContext.Current.CancellationToken);
        var rows = new List<object?[]>();
        while (await reader.ReadAsync(TestContext.Current.CancellationToken))
        {
            var row = new object?[reader.FieldCount];
            for (var i = 0; i < row.Length; i++)
            {
                row[i] = reader.IsDBNull(i) ? null : reader.GetValue(i);
            }

            rows.Add(row);
        }

        return rows;
    }

    /// <summary>Polls <paramref name="condition"/> until it holds or the timeout expires; returns the last result.</summary>
    public static async Task<bool> EventuallyAsync(Func<Task<bool>> condition, TimeSpan timeout)
    {
        var deadline = DateTime.UtcNow + timeout;
        while (true)
        {
            if (await condition())
            {
                return true;
            }

            if (DateTime.UtcNow >= deadline)
            {
                return false;
            }

            await Task.Delay(250, TestContext.Current.CancellationToken);
        }
    }

    private static NpgsqlCommand Command(NpgsqlDataSource dataSource, string sql, object?[] args)
    {
        var command = dataSource.CreateCommand(sql);
        foreach (var arg in args)
        {
            command.Parameters.Add(new NpgsqlParameter { Value = arg ?? DBNull.Value });
        }

        return command;
    }

    private static T Convert<T>(object? value)
    {
        if (value is null or DBNull)
        {
            return default!;
        }

        if (value is T typed)
        {
            return typed;
        }

        var target = Nullable.GetUnderlyingType(typeof(T)) ?? typeof(T);
        return (T)System.Convert.ChangeType(value, target, CultureInfo.InvariantCulture);
    }
}
