using System.Text.Json;
using Npgsql;
using NpgsqlTypes;
using T1.Core.Database;

namespace T1.Core.Operations;

/// <summary>Operations on T1 (writes go to the primary).</summary>
public sealed class OperationsRepository(T1DataSources db)
{
    private const string Columns = "id, op_date, amount, state, operation_guid, message::text";

    /// <summary>Requirement 4: t1_add_operation(), state = 0. A duplicate guid throws PostgresException 23505.</summary>
    public async Task<Operation> AddOperationAsync(NewOperation operation, CancellationToken ct = default)
    {
        await using var command = db.Primary.CreateCommand($"""
            SELECT {Columns}
              FROM t1_add_operation(p_amount         => @amount,
                                    p_client_id      => @client_id,
                                    p_operation_type => @operation_type,
                                    p_account_number => @account_number,
                                    p_operation_guid => @operation_guid,
                                    p_op_date        => @op_date)
            """);
        command.Parameters.Add(Sql.Param("amount", NpgsqlDbType.Numeric, operation.Amount));
        command.Parameters.Add(Sql.Param("client_id", NpgsqlDbType.Bigint, operation.ClientId));
        command.Parameters.Add(Sql.Param("operation_type", NpgsqlDbType.Text, operation.OperationType));
        command.Parameters.Add(Sql.Param("account_number", NpgsqlDbType.Text, operation.AccountNumber));
        command.Parameters.Add(Sql.Param("operation_guid", NpgsqlDbType.Uuid, operation.OperationGuid));
        command.Parameters.Add(Sql.Param("op_date", NpgsqlDbType.TimestampTz, operation.OpDate));

        await using var reader = await command.ExecuteReaderAsync(ct);
        await reader.ReadAsync(ct);
        return ReadOperation(reader);
    }

    /// <summary>
    /// Requirement 5: state 0 -> 1 for even ids when the second is even, odd ids otherwise.
    /// The second is taken from <paramref name="at"/>, or from the database clock when it is null.
    /// </summary>
    public async Task<ConfirmResult> ConfirmOperationsAsync(DateTimeOffset? at = null, CancellationToken ct = default)
    {
        await using var command = db.Primary.CreateCommand("SELECT second_of_minute, parity, confirmed_count FROM t1_confirm_operations(@at)");
        command.Parameters.Add(Sql.Param("at", NpgsqlDbType.TimestampTz, at?.UtcDateTime));

        await using var reader = await command.ExecuteReaderAsync(ct);
        await reader.ReadAsync(ct);
        return new ConfirmResult(reader.GetInt32(0), reader.GetString(1), reader.GetInt64(2));
    }

    public async Task<IReadOnlyList<Operation>> GetLatestAsync(int limit, CancellationToken ct = default)
    {
        await using var command = db.Primary.CreateCommand($"SELECT {Columns} FROM t1 ORDER BY id DESC LIMIT @limit");
        command.Parameters.Add(Sql.Param("limit", NpgsqlDbType.Integer, limit));
        return await ReadOperationsAsync(command, ct);
    }

    /// <summary>The registry gives (id, op_date), so only one partition is read.</summary>
    public async Task<Operation?> FindByGuidAsync(Guid operationGuid, CancellationToken ct = default)
    {
        await using var command = db.Primary.CreateCommand($"""
            SELECT {string.Join(", ", Columns.Split(", ").Select(c => "t." + c))}
              FROM t1_operation_guid g
              JOIN t1 t ON t.id = g.t1_id AND t.op_date = g.op_date AND t.operation_guid = g.operation_guid
             WHERE g.operation_guid = @guid
            """);
        command.Parameters.Add(Sql.Param("guid", NpgsqlDbType.Uuid, operationGuid));
        var rows = await ReadOperationsAsync(command, ct);
        return rows.Count == 0 ? null : rows[0];
    }

    /// <summary>Requirement 6: the materialized view t1_client_totals.</summary>
    public async Task<IReadOnlyList<ClientTotal>> GetClientTotalsAsync(long? clientId, string? operationType, int limit, CancellationToken ct = default)
    {
        await using var command = db.Primary.CreateCommand("""
            SELECT client_id, operation_type, total_amount, operations_count
              FROM t1_client_totals
             WHERE (@client_id::bigint IS NULL OR client_id = @client_id)
               AND (@operation_type::text IS NULL OR operation_type = @operation_type)
             ORDER BY client_id, operation_type
             LIMIT @limit
            """);
        command.Parameters.Add(Sql.Param("client_id", NpgsqlDbType.Bigint, clientId));
        command.Parameters.Add(Sql.Param("operation_type", NpgsqlDbType.Text, operationType));
        command.Parameters.Add(Sql.Param("limit", NpgsqlDbType.Integer, limit));

        await using var reader = await command.ExecuteReaderAsync(ct);
        var result = new List<ClientTotal>();
        while (await reader.ReadAsync(ct))
        {
            result.Add(new ClientTotal(reader.GetInt64(0), reader.GetString(1), reader.GetDecimal(2), reader.GetInt64(3)));
        }

        return result;
    }

    public async Task<ClientTotalsSummary> GetClientTotalsSummaryAsync(CancellationToken ct = default)
    {
        await using var command = db.Primary.CreateCommand(
            "SELECT count(*), coalesce(sum(total_amount), 0), coalesce(sum(operations_count), 0)::bigint FROM t1_client_totals");
        await using var reader = await command.ExecuteReaderAsync(ct);
        await reader.ReadAsync(ct);
        return new ClientTotalsSummary(reader.GetInt64(0), reader.GetDecimal(1), reader.GetInt64(2));
    }

    public static async Task<TableSnapshot> GetSnapshotAsync(NpgsqlDataSource dataSource, CancellationToken ct = default)
    {
        await using var command = dataSource.CreateCommand("""
            SELECT count(*),
                   count(*) FILTER (WHERE state = 0),
                   count(*) FILTER (WHERE state = 1),
                   max(id),
                   coalesce(sum(amount), 0),
                   min(op_date),
                   max(op_date)
              FROM t1
            """);
        await using var reader = await command.ExecuteReaderAsync(ct);
        await reader.ReadAsync(ct);
        return new TableSnapshot(
            reader.GetInt64(0),
            reader.GetInt64(1),
            reader.GetInt64(2),
            reader.IsDBNull(3) ? null : reader.GetInt64(3),
            reader.GetDecimal(4),
            reader.IsDBNull(5) ? null : reader.GetFieldValue<DateTime>(5),
            reader.IsDBNull(6) ? null : reader.GetFieldValue<DateTime>(6));
    }

    private static async Task<IReadOnlyList<Operation>> ReadOperationsAsync(NpgsqlCommand command, CancellationToken ct)
    {
        await using var reader = await command.ExecuteReaderAsync(ct);
        var result = new List<Operation>();
        while (await reader.ReadAsync(ct))
        {
            result.Add(ReadOperation(reader));
        }

        return result;
    }

    private static Operation ReadOperation(NpgsqlDataReader reader) => new(
        reader.GetInt64(0),
        reader.GetFieldValue<DateTime>(1),
        reader.GetDecimal(2),
        reader.GetInt16(3),
        reader.GetGuid(4),
        JsonSerializer.Deserialize<OperationMessage>(reader.GetString(5))
            ?? throw new InvalidOperationException("t1.message is null"));
}
