using Npgsql;
using T1.Core.Operations;
using T1.IntegrationTests.Infrastructure;

namespace T1.IntegrationTests;

/// <summary>1. Partitioned table T1: date, id, amount, state, operation guid, message (jsonb: account, client id, online/offline).</summary>
public sealed class Req1_PartitionedTableTests(PostgresClusterFixture cluster) : IAsyncLifetime
{
    private TestDatabases _db = null!;

    public async ValueTask InitializeAsync()
    {
        _db = await cluster.CreateDatabasesAsync("req1");
        await _db.MigratePrimaryAsync();
        await _db.Primary.ScalarAsync<int>("SELECT t1_ensure_partitions(now(), now())");
    }

    public async ValueTask DisposeAsync() => await _db.DisposeAsync();

    private CancellationToken Ct => TestContext.Current.CancellationToken;

    [Fact]
    public async Task T1_is_range_partitioned_by_op_date()
    {
        var row = Assert.Single(await _db.Primary.RowsAsync("""
            SELECT p.partstrat::text, a.attname::text
              FROM pg_partitioned_table p
              JOIN pg_attribute a ON a.attrelid = p.partrelid AND a.attnum = p.partattrs[0]
             WHERE p.partrelid = 't1'::regclass
            """));

        Assert.Equal("r", row[0]);
        Assert.Equal("op_date", row[1]);
    }

    [Fact]
    public async Task T1_has_the_required_columns()
    {
        var columns = (await _db.Primary.RowsAsync("""
                SELECT attname::text, format_type(atttypid, atttypmod), attnotnull, attidentity::text
                  FROM pg_attribute
                 WHERE attrelid = 't1'::regclass AND attnum > 0 AND NOT attisdropped
                 ORDER BY attnum
                """))
            .Select(r => $"{r[0]} {r[1]}{((bool)r[2]! ? " not null" : "")}{((string)r[3]! == "a" ? " identity" : "")}");

        Assert.Equal(
            [
                "id bigint not null identity",
                "op_date timestamp with time zone not null",
                "amount numeric(18,2) not null",
                "state smallint not null",
                "operation_guid uuid not null",
                "message jsonb not null",
            ],
            columns);
    }

    [Fact]
    public async Task Monthly_partitions_have_utc_boundaries_whatever_the_session_time_zone()
    {
        await using var connection = await _db.Primary.OpenConnectionAsync(Ct);
        await using (var command = new NpgsqlCommand("SET TimeZone = 'Europe/Kyiv'", connection))
        {
            await command.ExecuteNonQueryAsync(Ct);
        }

        // October -> November crosses the DST switch in Kyiv
        await using (var command = new NpgsqlCommand("SELECT t1_ensure_partitions('2031-10-15 12:00+03', '2031-11-20 12:00+02')", connection))
        {
            Assert.Equal(2, await command.ExecuteScalarAsync(Ct));
            Assert.Equal(0, await command.ExecuteScalarAsync(Ct));
        }

        var bounds = await _db.Primary.RowsAsync("""
            SELECT c.relname::text, pg_get_expr(c.relpartbound, c.oid)
              FROM pg_inherits i JOIN pg_class c ON c.oid = i.inhrelid
             WHERE i.inhparent = 't1'::regclass AND c.relname IN ('t1_2031_10', 't1_2031_11')
             ORDER BY 1
            """);

        Assert.Equal(
            [
                "t1_2031_10: FOR VALUES FROM ('2031-10-01 00:00:00+00') TO ('2031-11-01 00:00:00+00')",
                "t1_2031_11: FOR VALUES FROM ('2031-11-01 00:00:00+00') TO ('2031-12-01 00:00:00+00')",
            ],
            bounds.Select(r => $"{r[0]}: {r[1]}"));
    }

    [Fact]
    public async Task Rows_are_stored_in_the_partition_of_their_month()
    {
        await _db.Primary.ScalarAsync<int>("SELECT t1_ensure_partitions('2032-01-01', '2032-03-31')");

        var january = await _db.Operations.AddOperationAsync(new NewOperation(OpDate: new DateTime(2032, 1, 31, 23, 59, 59, DateTimeKind.Utc)), Ct);
        var february = await _db.Operations.AddOperationAsync(new NewOperation(OpDate: new DateTime(2032, 2, 1, 0, 0, 0, DateTimeKind.Utc)), Ct);
        var march = await _db.Operations.AddOperationAsync(new NewOperation(OpDate: new DateTime(2032, 3, 15, 0, 0, 0, DateTimeKind.Utc)), Ct);

        async Task<string> PartitionOf(long id) => await _db.Primary.ScalarAsync<string>("SELECT tableoid::regclass::text FROM t1 WHERE id = $1", id);

        Assert.Equal("t1_2032_01", await PartitionOf(january.Id));
        Assert.Equal("t1_2032_02", await PartitionOf(february.Id));
        Assert.Equal("t1_2032_03", await PartitionOf(march.Id));
    }

    [Fact]
    public async Task New_partition_gets_identity_triggers_indexes_and_checks_of_t1()
    {
        await _db.Primary.ScalarAsync<int>("SELECT t1_ensure_partitions('2033-01-15', '2033-01-15')");

        var id = await _db.Primary.ScalarAsync<long>(
            "INSERT INTO t1_2033_01 (op_date, amount, operation_guid, message) VALUES ('2033-01-20', 5, gen_random_uuid(), t1_make_message(1, 1, 'online')) RETURNING id");
        Assert.True(id > 0);
        Assert.Equal(5L, await _db.Primary.ScalarAsync<long>("SELECT count(*) FROM pg_trigger WHERE tgrelid = 't1_2033_01'::regclass"));
        Assert.Equal(3L, await _db.Primary.ScalarAsync<long>("SELECT count(*) FROM pg_index WHERE indrelid = 't1_2033_01'::regclass"));
        Assert.Equal(1L, await _db.Primary.ScalarAsync<long>("SELECT count(*) FROM t1_operation_guid WHERE t1_id = $1", id));
    }

    [Fact]
    public async Task Creating_a_partition_does_not_wait_for_readers_of_t1()
    {
        // CREATE + ATTACH needs only SHARE UPDATE EXCLUSIVE on t1, which does not conflict with a running SELECT
        await using var reader = await _db.Primary.OpenConnectionAsync(Ct);
        await using var transaction = await reader.BeginTransactionAsync(Ct);
        await using (var select = new NpgsqlCommand("SELECT count(*) FROM t1", reader, transaction))
        {
            await select.ExecuteScalarAsync(Ct);
        }

        await using var connection = await _db.Primary.OpenConnectionAsync(Ct);
        await using (var timeout = new NpgsqlCommand("SET statement_timeout = '3s'", connection))
        {
            await timeout.ExecuteNonQueryAsync(Ct);
        }

        await using var command = new NpgsqlCommand("SELECT t1_ensure_partitions('2034-05-01', '2034-05-01')", connection);
        Assert.Equal(1, await command.ExecuteScalarAsync(Ct));
    }

    [Fact]
    public async Task Detached_month_is_reported_instead_of_silently_skipped()
    {
        await _db.Primary.ScalarAsync<int>("SELECT t1_ensure_partitions('2035-03-01', '2035-03-01')");
        await _db.Primary.ExecuteAsync("ALTER TABLE t1 DETACH PARTITION t1_2035_03");

        var error = await Assert.ThrowsAsync<PostgresException>(() => _db.Primary.ExecuteAsync("SELECT t1_ensure_partitions('2035-03-01', '2035-03-01')"));

        Assert.Contains("not a partition of t1", error.MessageText, StringComparison.Ordinal);
    }

    [Fact]
    public async Task Partition_months_ignore_a_default_partition()
    {
        var before = await _db.Primary.ScalarAsync<long>("SELECT count(*) FROM t1_partition_months()");
        await _db.Primary.ExecuteAsync("CREATE TABLE t1_default PARTITION OF t1 DEFAULT");

        Assert.Equal(before, await _db.Primary.ScalarAsync<long>("SELECT count(*) FROM t1_partition_months()"));
    }

    [Theory]
    [InlineData("'infinity', 'infinity'")]
    [InlineData("'2030-01-01', '2040-01-01'")]
    [InlineData("'2030-02-01', '2030-01-01'")]
    public async Task Invalid_partition_ranges_are_rejected(string arguments)
    {
        await Assert.ThrowsAsync<PostgresException>(() => _db.Primary.ExecuteAsync($"SELECT t1_ensure_partitions({arguments})"));
    }

    [Fact]
    public async Task Operation_outside_of_existing_partitions_is_rejected()
    {
        var error = await Assert.ThrowsAsync<PostgresException>(() =>
            _db.Operations.AddOperationAsync(new NewOperation(OpDate: new DateTime(1999, 1, 1, 0, 0, 0, DateTimeKind.Utc)), Ct));

        Assert.Equal(PostgresErrorCodes.CheckViolation, error.SqlState); // no partition of relation "t1" found for row
    }

    [Fact]
    public async Task Message_contains_account_number_client_id_and_operation_type()
    {
        var operation = await _db.Operations.AddOperationAsync(new NewOperation(ClientId: 42, OperationType: OperationTypes.Offline), Ct);

        Assert.Equal(42, operation.Message.ClientId);
        Assert.Equal(OperationTypes.Offline, operation.Message.OperationType);
        Assert.Matches("^UA[0-9]{27}$", operation.Message.AccountNumber);
        Assert.Equal("account_number,client_id,operation_type",
            await _db.Primary.ScalarAsync<string>("SELECT string_agg(k, ',' ORDER BY k) FROM t1, jsonb_object_keys(message) k WHERE id = $1", operation.Id));
    }

    [Theory]
    [InlineData("""{"account_number": "UA1", "client_id": 1, "operation_type": "cash"}""")]
    [InlineData("""{"account_number": "UA1", "operation_type": "online"}""")]
    [InlineData("""{"client_id": 1, "operation_type": "online"}""")]
    [InlineData("""{"account_number": "UA1", "client_id": 1.5, "operation_type": "online"}""")]
    [InlineData("""{"account_number": "UA1", "client_id": "1", "operation_type": "online"}""")]
    [InlineData("""{"account_number": " ", "client_id": 1, "operation_type": "online"}""")]
    [InlineData("""["UA1", 1, "online"]""")]
    public async Task Malformed_message_is_rejected(string message)
    {
        var error = await Assert.ThrowsAsync<PostgresException>(() => _db.Primary.ExecuteAsync(
            "INSERT INTO t1 (op_date, amount, operation_guid, message) VALUES (now(), 1, gen_random_uuid(), $1::jsonb)", message));

        Assert.Equal("t1_message_chk", error.ConstraintName);
    }

    [Theory]
    [InlineData("0")]
    [InlineData("-5")]
    [InlineData("'NaN'")]
    public async Task Amount_must_be_a_positive_number(string amount)
    {
        var error = await Assert.ThrowsAsync<PostgresException>(() => _db.Primary.ExecuteAsync(
            $"INSERT INTO t1 (op_date, amount, operation_guid, message) VALUES (now(), {amount}, gen_random_uuid(), t1_make_message(1, 1, 'online'))"));

        Assert.Equal("t1_amount_chk", error.ConstraintName);
    }

    [Fact]
    public async Task State_is_0_or_1()
    {
        var operation = await _db.Operations.AddOperationAsync(new NewOperation(), Ct);

        var error = await Assert.ThrowsAsync<PostgresException>(() => _db.Primary.ExecuteAsync("UPDATE t1 SET state = 2 WHERE id = $1", operation.Id));

        Assert.Equal("t1_state_chk", error.ConstraintName);
    }
}
