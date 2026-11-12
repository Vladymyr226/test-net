using Npgsql;
using T1.Core.Operations;
using T1.IntegrationTests.Infrastructure;

namespace T1.IntegrationTests;

/// <summary>6. Totals of "amount" per client id and operation type in a materialized view, refreshed on every 0 -> 1 change of "state".</summary>
public sealed class Req6_ClientTotalsTests(PostgresClusterFixture cluster) : IAsyncLifetime
{
    private TestDatabases _db = null!;

    public async ValueTask InitializeAsync()
    {
        _db = await cluster.CreateDatabasesAsync("req6");
        await _db.MigratePrimaryAsync();
        await _db.Primary.ExecuteAsync("CALL t1_generate_data(p_rows => 5000, p_from => now() - interval '2 months', p_to => now())");
    }

    public async ValueTask DisposeAsync() => await _db.DisposeAsync();

    private CancellationToken Ct => TestContext.Current.CancellationToken;

    [Fact]
    public async Task Totals_is_a_materialized_view()
    {
        Assert.Equal("m", await _db.Primary.ScalarAsync<string>("SELECT relkind::text FROM pg_class WHERE oid = 't1_client_totals'::regclass"));
    }

    [Fact]
    public async Task New_operation_is_not_counted_until_confirmed_and_confirmation_refreshes_the_view()
    {
        var operation = await Add(900_001, OperationTypes.Online, 250.75m);
        Assert.Null(await Total(900_001, OperationTypes.Online));

        await _db.Operations.ConfirmOperationsAsync(SecondWithParityOf(operation.Id), Ct);

        Assert.Equal((250.75m, 1L), await Total(900_001, OperationTypes.Online));
        await AssertViewMatchesTable();
    }

    [Fact]
    public async Task Any_update_from_0_to_1_refreshes_the_view_including_updates_of_a_partition()
    {
        var viaParent = await Add(900_002, OperationTypes.Offline, 10m);
        var viaPartition = await Add(900_002, OperationTypes.Offline, 32m);

        await _db.Primary.ExecuteAsync("UPDATE t1 SET state = 1 WHERE id = $1", viaParent.Id);
        Assert.Equal((10m, 1L), await Total(900_002, OperationTypes.Offline));

        await _db.Primary.ExecuteAsync($"UPDATE t1_{viaPartition.OpDate:yyyy_MM} SET state = 1 WHERE id = $1", viaPartition.Id);
        Assert.Equal((42m, 2L), await Total(900_002, OperationTypes.Offline));
    }

    [Fact]
    public async Task Several_confirmations_in_one_transaction_are_all_counted_at_commit()
    {
        var a = await Add(900_003, OperationTypes.Online, 1m);
        var b = await Add(900_003, OperationTypes.Online, 2m);

        await InTransaction(async (connection, transaction) =>
        {
            foreach (var id in new[] { a.Id, b.Id })
            {
                await Execute(connection, transaction, $"UPDATE t1 SET state = 1 WHERE id = {id}");
            }
        });

        Assert.Equal((3m, 2L), await Total(900_003, OperationTypes.Online));
    }

    [Fact]
    public async Task Immediate_mode_counts_every_batch_of_one_command()
    {
        var a = await Add(900_006, OperationTypes.Online, 1m);
        var b = await Add(900_006, OperationTypes.Online, 10m);

        await InTransaction(async (connection, transaction) =>
        {
            await Execute(connection, transaction, "SET CONSTRAINTS ALL IMMEDIATE");
            await Execute(connection, transaction, $"DO $$ BEGIN UPDATE t1 SET state = 1 WHERE id = {a.Id}; UPDATE t1 SET state = 1 WHERE id = {b.Id}; END $$");
        });

        Assert.Equal((11m, 2L), await Total(900_006, OperationTypes.Online));
    }

    [Theory]
    [InlineData("RESET ALL")]
    [InlineData("SET LOCAL t1.client_totals_refreshed = 'off'")]
    public async Task Session_settings_cannot_skip_the_refresh(string statement)
    {
        var clientId = 900_010 + statement.Length;
        var operation = await Add(clientId, OperationTypes.Online, 100m);

        await InTransaction(async (connection, transaction) =>
        {
            await Execute(connection, transaction, $"UPDATE t1 SET state = 1 WHERE id = {operation.Id}");
            await Execute(connection, transaction, statement);
        });

        Assert.Equal((100m, 1L), await Total(clientId, OperationTypes.Online));
    }

    [Fact]
    public async Task Rolled_back_confirmations_change_nothing()
    {
        var operation = await Add(900_004, OperationTypes.Online, 5m);

        await using (var connection = await _db.Primary.OpenConnectionAsync(Ct))
        await using (var transaction = await connection.BeginTransactionAsync(Ct))
        {
            await Execute(connection, transaction, $"UPDATE t1 SET state = 1 WHERE id = {operation.Id}");
            await transaction.RollbackAsync(Ct);
        }

        await InTransaction(async (connection, transaction) =>
        {
            await Execute(connection, transaction, "SAVEPOINT s");
            await Execute(connection, transaction, $"UPDATE t1 SET state = 1 WHERE id = {operation.Id}");
            await Execute(connection, transaction, "ROLLBACK TO SAVEPOINT s");
        });

        Assert.Null(await Total(900_004, OperationTypes.Online));
        Assert.Equal(0, await _db.Primary.ScalarAsync<short>("SELECT state FROM t1 WHERE id = $1", operation.Id));
    }

    [Fact]
    public async Task Concurrent_confirmations_are_all_reflected()
    {
        var operations = new List<Operation>();
        for (var i = 1; i <= 6; i++)
        {
            operations.Add(await Add(900_005, OperationTypes.Offline, i * 100m));
        }

        // the transactions hold their changes for a while, so their refreshes at commit overlap
        await Task.WhenAll(operations.Select(operation => Task.Run(() => InTransaction(async (connection, transaction) =>
        {
            await Execute(connection, transaction, $"UPDATE t1 SET state = 1 WHERE id = {operation.Id}");
            await Execute(connection, transaction, "SELECT pg_sleep(0.3)");
        }), Ct)));

        Assert.Equal((2100m, 6L), await Total(900_005, OperationTypes.Offline));
        await AssertViewMatchesTable();
    }

    [Fact]
    public async Task Refresh_by_the_generator_does_not_lose_a_concurrent_confirmation()
    {
        var operation = await Add(900_007, OperationTypes.Online, 500m);

        // the confirmation refreshes right away (IMMEDIATE) and keeps its transaction open for a while;
        // the generator's refresh has to wait for it and then see it
        var confirmation = InTransaction(async (connection, transaction) =>
        {
            await Execute(connection, transaction, "SET CONSTRAINTS ALL IMMEDIATE");
            await Execute(connection, transaction, $"UPDATE t1 SET state = 1 WHERE id = {operation.Id}");
            await Execute(connection, transaction, "SELECT pg_sleep(2)");
        });
        await Task.Delay(500, Ct);
        await _db.Primary.ExecuteAsync("CALL t1_generate_data(p_rows => 10, p_from => now() - interval '1 hour', p_to => now(), p_pending_share => 0)");
        await confirmation;

        Assert.Equal((500m, 1L), await Total(900_007, OperationTypes.Online));
        await AssertViewMatchesTable();
    }

    [Fact]
    public async Task Confirmations_must_run_in_read_committed()
    {
        var operation = await Add(900_008, OperationTypes.Online, 1m);

        var error = await Assert.ThrowsAsync<PostgresException>(() => InTransaction(
            (connection, transaction) => Execute(connection, transaction, $"UPDATE t1 SET state = 1 WHERE id = {operation.Id}"),
            System.Data.IsolationLevel.RepeatableRead));

        Assert.Equal(PostgresErrorCodes.FeatureNotSupported, error.SqlState);
    }

    [Fact]
    public async Task Other_roles_cannot_trigger_refreshes_of_the_view()
    {
        await using var role = await _db.CreateDmlRoleAsync();
        var operation = await Add(900_009, OperationTypes.Offline, 9m);

        await role.ExecuteAsync("UPDATE t1 SET state = 1 WHERE id = $1", operation.Id);   // allowed: the trigger runs as the owner
        Assert.Equal((9m, 1L), await Total(900_009, OperationTypes.Offline));

        await Assert.ThrowsAsync<PostgresException>(() => role.ExecuteAsync("SELECT t1_refresh_client_totals()"));
        await Assert.ThrowsAsync<PostgresException>(() => role.ExecuteAsync("""
            CREATE TEMP TABLE y (state int);
            CREATE TRIGGER y_refresh AFTER UPDATE ON y FOR EACH ROW EXECUTE FUNCTION public.t1_client_totals_refresh();
            """));
    }

    [Fact]
    public async Task Confirming_seeded_pending_operations_keeps_the_view_exact()
    {
        await _db.Operations.ConfirmOperationsAsync(new DateTimeOffset(2026, 1, 1, 0, 0, 2, TimeSpan.Zero), Ct);
        await _db.Operations.ConfirmOperationsAsync(new DateTimeOffset(2026, 1, 1, 0, 0, 3, TimeSpan.Zero), Ct);

        Assert.Equal(0L, await _db.Primary.ScalarAsync<long>("SELECT count(*) FROM t1 WHERE state = 0"));
        await AssertViewMatchesTable();
    }

    private Task<Operation> Add(long clientId, string type, decimal amount) =>
        _db.Operations.AddOperationAsync(new NewOperation(Amount: amount, ClientId: clientId, OperationType: type), Ct);

    private static DateTimeOffset SecondWithParityOf(long id) => new(2026, 1, 1, 0, 0, id % 2 == 0 ? 10 : 11, TimeSpan.Zero);

    private async Task InTransaction(Func<NpgsqlConnection, NpgsqlTransaction, Task> body, System.Data.IsolationLevel isolation = System.Data.IsolationLevel.ReadCommitted)
    {
        await using var connection = await _db.Primary.OpenConnectionAsync(Ct);
        await using var transaction = await connection.BeginTransactionAsync(isolation, Ct);
        await body(connection, transaction);
        await transaction.CommitAsync(Ct);
    }

    private async Task Execute(NpgsqlConnection connection, NpgsqlTransaction transaction, string sql)
    {
        await using var command = new NpgsqlCommand(sql, connection, transaction);
        await command.ExecuteNonQueryAsync(Ct);
    }

    private async Task<(decimal Amount, long Count)?> Total(long clientId, string operationType)
    {
        var rows = await _db.Primary.RowsAsync(
            "SELECT total_amount, operations_count FROM t1_client_totals WHERE client_id = $1 AND operation_type = $2", clientId, operationType);
        return rows.Count == 0 ? null : ((decimal)rows[0][0]!, (long)rows[0][1]!);
    }

    private async Task AssertViewMatchesTable() =>
        Assert.Equal(0L, await _db.Primary.ScalarAsync<long>("""
            WITH live AS (
                SELECT (message ->> 'client_id')::bigint, message ->> 'operation_type', sum(amount), count(*)
                  FROM t1 WHERE state = 1 GROUP BY 1, 2)
            SELECT count(*) FROM ((SELECT * FROM live EXCEPT SELECT * FROM t1_client_totals)
                                  UNION ALL (SELECT * FROM t1_client_totals EXCEPT SELECT * FROM live)) d
            """));
}
