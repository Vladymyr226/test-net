using System.Diagnostics;
using Microsoft.Extensions.Logging;
using Npgsql;
using NpgsqlTypes;
using T1.Core.Database;

namespace T1.Core.Seeding;

public sealed class SeedOptions
{
    public bool Enabled { get; set; } = true;

    public int Rows { get; set; } = 120_000;

    /// <summary>Period: from the 1st day of the month <c>Months</c> months ago (UTC) to now.</summary>
    public int Months { get; set; } = 3;

    public int Clients { get; set; } = 1000;

    /// <summary>Share of generated rows left with state = 0.</summary>
    public decimal PendingShare { get; set; } = 0.01m;

    public int BatchSize { get; set; } = 20_000;
}

/// <summary>Requirement 2: fills an empty t1 through the stored procedure t1_generate_data.</summary>
public sealed class DataSeeder(T1DataSources db, SeedOptions options, ILogger<DataSeeder> logger)
{
    private const long LockKey = 7_310_002;

    /// <returns>Number of generated rows; 0 when seeding is disabled or t1 already has data.</returns>
    public async Task<int> SeedIfEmptyAsync(CancellationToken ct = default)
    {
        if (!options.Enabled)
        {
            return 0;
        }

        await using var connection = await db.Primary.OpenConnectionAsync(ct);
        await using var _ = await Sql.AdvisoryLockAsync(connection, LockKey, ct);

        if (await Sql.ScalarAsync<bool>(connection, "SELECT EXISTS (SELECT 1 FROM t1)", ct))
        {
            logger.LogInformation("t1 already contains data, seeding skipped");
            return 0;
        }

        logger.LogInformation("Generating {Rows} rows for {Months} months back: CALL t1_generate_data(...)", options.Rows, options.Months);
        var stopwatch = Stopwatch.StartNew();

        // One transaction: an interrupted seed leaves nothing behind and is simply repeated on the next start.
        await using var transaction = await connection.BeginTransactionAsync(ct);
        await GenerateAsync(connection, options, commitPerBatch: false, ct);
        await transaction.CommitAsync(ct);

        logger.LogInformation("Generated {Rows} rows in {Elapsed:F1} s", options.Rows, stopwatch.Elapsed.TotalSeconds);
        return options.Rows;
    }

    public async Task GenerateAsync(NpgsqlConnection connection, SeedOptions seed, bool commitPerBatch, CancellationToken ct)
    {
        void OnNotice(object sender, NpgsqlNoticeEventArgs e) => logger.LogInformation("{Notice}", e.Notice.MessageText);

        connection.Notice += OnNotice;
        try
        {
            await using var command = new NpgsqlCommand("""
                CALL t1_generate_data(
                    p_rows          => @rows,
                    p_from          => (date_trunc('month', now() AT TIME ZONE 'UTC') - make_interval(months => @months)) AT TIME ZONE 'UTC',
                    p_to            => now(),
                    p_clients       => @clients,
                    p_pending_share => @pending_share,
                    p_batch_size    => @batch_size,
                    p_commit        => @commit)
                """, connection) { CommandTimeout = 0 };
            command.Parameters.Add(Sql.Param("rows", NpgsqlDbType.Integer, seed.Rows));
            command.Parameters.Add(Sql.Param("months", NpgsqlDbType.Integer, seed.Months));
            command.Parameters.Add(Sql.Param("clients", NpgsqlDbType.Integer, seed.Clients));
            command.Parameters.Add(Sql.Param("pending_share", NpgsqlDbType.Numeric, seed.PendingShare));
            command.Parameters.Add(Sql.Param("batch_size", NpgsqlDbType.Integer, seed.BatchSize));
            command.Parameters.Add(Sql.Param("commit", NpgsqlDbType.Boolean, commitPerBatch));
            await command.ExecuteNonQueryAsync(ct);
        }
        finally
        {
            connection.Notice -= OnNotice;
        }
    }
}
