using Npgsql;
using T1.Core.Operations;
using T1.IntegrationTests.Infrastructure;

namespace T1.IntegrationTests;

/// <summary>3. Uniqueness of the operation guid across the whole partitioned table.</summary>
public sealed class Req3_OperationGuidUniquenessTests(PostgresClusterFixture cluster) : IAsyncLifetime
{
    private static readonly DateTime OtherMonth = DateTime.UtcNow.AddMonths(-2);

    private TestDatabases _db = null!;

    public async ValueTask InitializeAsync()
    {
        _db = await cluster.CreateDatabasesAsync("req3");
        await _db.MigratePrimaryAsync();
        await _db.Primary.ScalarAsync<int>("SELECT t1_ensure_partitions(now() - interval '3 months', now())");
    }

    public async ValueTask DisposeAsync() => await _db.DisposeAsync();

    private CancellationToken Ct => TestContext.Current.CancellationToken;

    [Fact]
    public async Task Plain_unique_constraint_on_the_guid_is_impossible_on_a_partitioned_table()
    {
        var error = await Assert.ThrowsAsync<PostgresException>(() => _db.Primary.ExecuteAsync("ALTER TABLE t1 ADD UNIQUE (operation_guid)"));

        Assert.Equal(PostgresErrorCodes.FeatureNotSupported, error.SqlState);
    }

    [Fact]
    public async Task Duplicate_guid_in_the_same_partition_is_rejected()
    {
        var first = await _db.Operations.AddOperationAsync(new NewOperation(), Ct);

        var error = await Assert.ThrowsAsync<PostgresException>(() =>
            _db.Operations.AddOperationAsync(new NewOperation(OperationGuid: first.OperationGuid, OpDate: first.OpDate), Ct));

        Assert.Equal(PostgresErrorCodes.UniqueViolation, error.SqlState);
        Assert.Equal("t1_operation_guid_pkey", error.ConstraintName);
        Assert.Equal(1L, await CountRowsWithGuid(first.OperationGuid));
    }

    [Fact]
    public async Task Duplicate_guid_in_another_partition_is_rejected()
    {
        var first = await _db.Operations.AddOperationAsync(new NewOperation(), Ct);

        var error = await Assert.ThrowsAsync<PostgresException>(() =>
            _db.Operations.AddOperationAsync(new NewOperation(OperationGuid: first.OperationGuid, OpDate: OtherMonth), Ct));

        Assert.Equal(PostgresErrorCodes.UniqueViolation, error.SqlState);
        Assert.Equal(1L, await CountRowsWithGuid(first.OperationGuid));
    }

    [Fact]
    public async Task Duplicate_guid_inserted_straight_into_a_partition_is_rejected()
    {
        var first = await _db.Operations.AddOperationAsync(new NewOperation(OpDate: OtherMonth), Ct);

        var error = await Assert.ThrowsAsync<PostgresException>(() => _db.Primary.ExecuteAsync(
            $"INSERT INTO t1_{DateTime.UtcNow:yyyy_MM} (op_date, amount, operation_guid, message) VALUES (now(), 1, $1, t1_make_message(1, 1, 'online'))",
            first.OperationGuid));

        Assert.Equal(PostgresErrorCodes.UniqueViolation, error.SqlState);
    }

    [Fact]
    public async Task Duplicate_guid_within_one_multi_row_insert_is_rejected()
    {
        var guid = Guid.CreateVersion7();

        var error = await Assert.ThrowsAsync<PostgresException>(() => _db.Primary.ExecuteAsync("""
            INSERT INTO t1 (op_date, amount, operation_guid, message)
            SELECT now() - make_interval(months => g), 1, $1, t1_make_message(1, 1, 'online') FROM generate_series(0, 1) g
            """, guid));

        Assert.Equal(PostgresErrorCodes.UniqueViolation, error.SqlState);
        Assert.Equal(0L, await CountRowsWithGuid(guid));
    }

    [Fact]
    public async Task Skipped_upsert_does_not_reserve_its_guid()
    {
        var existing = await _db.Operations.AddOperationAsync(new NewOperation(), Ct);
        var guid = Guid.CreateVersion7();

        Assert.Equal(0, await _db.Primary.ExecuteAsync("""
            INSERT INTO t1 (id, op_date, amount, operation_guid, message) OVERRIDING SYSTEM VALUE
            SELECT id, op_date, 1, $1, message FROM t1 WHERE id = $2
            ON CONFLICT (id, op_date) DO NOTHING
            """, guid, existing.Id));

        Assert.Equal(0L, await _db.Primary.ScalarAsync<long>("SELECT count(*) FROM t1_operation_guid WHERE operation_guid = $1", guid));
        await _db.Operations.AddOperationAsync(new NewOperation(OperationGuid: guid), Ct);   // still free
    }

    [Fact]
    public async Task Guid_can_be_changed_only_to_a_free_value()
    {
        var a = await _db.Operations.AddOperationAsync(new NewOperation(), Ct);
        var b = await _db.Operations.AddOperationAsync(new NewOperation(OpDate: OtherMonth), Ct);

        var error = await Assert.ThrowsAsync<PostgresException>(() =>
            _db.Primary.ExecuteAsync("UPDATE t1 SET operation_guid = $1 WHERE id = $2", a.OperationGuid, b.Id));
        Assert.Equal(PostgresErrorCodes.UniqueViolation, error.SqlState);

        var fresh = Guid.CreateVersion7();
        Assert.Equal(1, await _db.Primary.ExecuteAsync("UPDATE t1 SET operation_guid = $1 WHERE id = $2", fresh, b.Id));
        Assert.Equal(b.Id, (await _db.Operations.FindByGuidAsync(fresh, Ct))?.Id);
        Assert.Null(await _db.Operations.FindByGuidAsync(b.OperationGuid, Ct));
        await _db.Operations.AddOperationAsync(new NewOperation(OperationGuid: b.OperationGuid), Ct);   // the old value is free
    }

    [Theory]
    [InlineData("op_date = op_date - interval '2 months'")]
    [InlineData("id = DEFAULT")]
    public async Task Id_and_operation_date_are_immutable(string assignment)
    {
        var operation = await _db.Operations.AddOperationAsync(new NewOperation(), Ct);

        var error = await Assert.ThrowsAsync<PostgresException>(() => _db.Primary.ExecuteAsync($"UPDATE t1 SET {assignment} WHERE id = $1", operation.Id));

        Assert.Equal(PostgresErrorCodes.FeatureNotSupported, error.SqlState);
    }

    [Fact]
    public async Task Deleting_an_operation_releases_its_guid()
    {
        var operation = await _db.Operations.AddOperationAsync(new NewOperation(), Ct);
        await _db.Primary.ExecuteAsync("DELETE FROM t1 WHERE id = $1", operation.Id);

        var again = await _db.Operations.AddOperationAsync(new NewOperation(OperationGuid: operation.OperationGuid, OpDate: OtherMonth), Ct);

        Assert.Equal(operation.OperationGuid, again.OperationGuid);
    }

    [Theory]
    [InlineData("read committed")]
    [InlineData("repeatable read")]
    [InlineData("serializable")]
    public async Task Concurrent_inserts_of_the_same_guid_store_exactly_one_row(string isolation)
    {
        const int guids = 20;
        const int writersPerGuid = 4;
        var keys = Enumerable.Range(0, guids).Select(_ => Guid.CreateVersion7()).ToArray();
        var connected = 0;
        var go = new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);

        var attempts = keys.SelectMany(guid => Enumerable.Range(0, writersPerGuid).Select(writer => Task.Run(async () =>
        {
            await using var connection = await _db.Primary.OpenConnectionAsync(Ct);
            await using var transaction = await connection.BeginTransactionAsync(
                isolation switch { "repeatable read" => System.Data.IsolationLevel.RepeatableRead, "serializable" => System.Data.IsolationLevel.Serializable, _ => System.Data.IsolationLevel.ReadCommitted }, Ct);
            await using var command = new NpgsqlCommand("SELECT id FROM t1_add_operation(p_operation_guid => $1, p_op_date => $2)", connection, transaction);
            command.Parameters.Add(new NpgsqlParameter { Value = guid });
            command.Parameters.Add(new NpgsqlParameter { Value = writer % 2 == 0 ? DateTime.UtcNow : OtherMonth });   // different partitions

            if (Interlocked.Increment(ref connected) == guids * writersPerGuid)
            {
                go.SetResult();
            }

            await go.Task.WaitAsync(Ct);
            try
            {
                await command.ExecuteScalarAsync(Ct);
                await transaction.CommitAsync(Ct);
                return true;
            }
            catch (PostgresException e) when (e.SqlState == PostgresErrorCodes.UniqueViolation)
            {
                return false;   // the same 23505 under every isolation level
            }
        }, Ct))).ToArray();

        var results = await Task.WhenAll(attempts);

        Assert.Equal(guids, results.Count(ok => ok));
        Assert.Equal(guids, await _db.Primary.ScalarAsync<long>("SELECT count(*) FROM t1 WHERE operation_guid = ANY($1)", keys));
    }

    [Fact]
    public async Task Resync_repairs_the_registry_after_detach_and_attach_of_a_partition()
    {
        await _db.Primary.ExecuteAsync("CALL t1_generate_data(p_rows => 3000, p_from => '2030-08-01', p_to => '2030-10-01')");
        var august = await _db.Primary.ScalarAsync<long>("SELECT count(*) FROM t1_2030_08");

        await _db.Primary.ExecuteAsync("ALTER TABLE t1 DETACH PARTITION t1_2030_08");
        Assert.Equal(august, await ResyncRemoved());

        await _db.Primary.ExecuteAsync("ALTER TABLE t1 ATTACH PARTITION t1_2030_08 FOR VALUES FROM ('2030-08-01 00:00+00') TO ('2030-09-01 00:00+00')");
        var added = (await _db.Primary.RowsAsync("SELECT added FROM t1_operation_guid_resync()"))[0][0];
        Assert.Equal(august, added);
        Assert.Equal(0L, await _db.Primary.ScalarAsync<long>(
            "SELECT count(*) FROM (SELECT operation_guid, id, op_date FROM t1 EXCEPT SELECT operation_guid, t1_id, op_date FROM t1_operation_guid) missing"));

        async Task<long> ResyncRemoved() => (long)(await _db.Primary.RowsAsync("SELECT removed FROM t1_operation_guid_resync()"))[0][0]!;
    }

    [Fact]
    public async Task Other_roles_cannot_reuse_the_registry_trigger_functions()
    {
        await using var role = await _db.CreateDmlRoleAsync();

        // plain DML through t1 works: the triggers run as their owner
        var id = await role.ScalarAsync<long>("SELECT id FROM t1_add_operation(p_client_id => 3000001)");
        Assert.True(id > 0);

        // but the SECURITY DEFINER functions cannot be attached elsewhere to tamper with the registry
        var error = await Assert.ThrowsAsync<PostgresException>(() => role.ExecuteAsync("""
            CREATE TEMP TABLE x (operation_guid uuid);
            CREATE TRIGGER x_truncate AFTER TRUNCATE ON x FOR EACH STATEMENT EXECUTE FUNCTION public.t1_operation_guid_on_truncate();
            """));
        Assert.Equal(PostgresErrorCodes.InsufficientPrivilege, error.SqlState);
    }

    private Task<long> CountRowsWithGuid(Guid guid) => _db.Primary.ScalarAsync<long>("SELECT count(*) FROM t1 WHERE operation_guid = $1", guid);
}
