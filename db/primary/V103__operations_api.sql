-- Routines used by the service and the scheduled jobs.

-- Ukrainian IBAN with valid mod-97 check digits (test data only).
CREATE FUNCTION t1_iban(p_account numeric, p_bank_code text DEFAULT '322001')
RETURNS text
LANGUAGE sql
IMMUTABLE STRICT PARALLEL SAFE
AS $$
    SELECT 'UA'
        || lpad((98 - ((p_bank_code || lpad(p_account::text, 19, '0') || '301000')::numeric % 97))::text, 2, '0')
        || p_bank_code
        || lpad(p_account::text, 19, '0')
$$;

-- UUIDv7 whose timestamp is exactly p_at (millisecond part); the remaining bits come from uuidv7().
CREATE FUNCTION t1_uuidv7_at(p_at timestamptz)
RETURNS uuid
LANGUAGE sql
VOLATILE STRICT
AS $$
    SELECT CASE WHEN p_at >= '1970-01-01 00:00+00' AND p_at < '10000-01-01 00:00+00'   -- 48-bit unix ms
                THEN encode(overlay(uuid_send(uuidv7())
                                    PLACING substring(int8send(floor(extract(epoch FROM p_at) * 1000)::bigint) FROM 3)
                                    FROM 1 FOR 6), 'hex')::uuid
                ELSE uuidv7()
           END
$$;

-- t1.message; every client owns the accounts client_id * 10 + 1..3.
CREATE FUNCTION t1_make_message(p_client_id bigint, p_account_no integer, p_operation_type text)
RETURNS jsonb
LANGUAGE sql
IMMUTABLE STRICT PARALLEL SAFE
AS $$
    SELECT jsonb_build_object(
               'account_number', t1_iban(p_client_id::numeric * 10 + p_account_no),
               'client_id',      p_client_id,
               'operation_type', p_operation_type)
$$;


-- Requirement 2: test data, spread uniformly over [p_from, p_to) and inserted chronologically.
-- Commits per batch unless p_commit = false (then it can run inside a transaction and is atomic).
--   CALL t1_generate_data();   -- 120 000 rows from the 1st day of month M-3 (UTC) to now()
CREATE PROCEDURE t1_generate_data(
    p_rows          integer     DEFAULT 120000,
    p_from          timestamptz DEFAULT NULL,
    p_to            timestamptz DEFAULT NULL,
    p_clients       integer     DEFAULT 1000,
    p_pending_share numeric     DEFAULT 0.01,
    p_batch_size    integer     DEFAULT 20000,
    p_commit        boolean     DEFAULT true)
LANGUAGE plpgsql
AS $$
DECLARE
    v_from    timestamptz := coalesce(p_from, (date_trunc('month', now() AT TIME ZONE 'UTC') - interval '3 months') AT TIME ZONE 'UTC');
    v_to      timestamptz := coalesce(p_to, now());
    -- epoch seconds: interval arithmetic on timestamptz depends on the session TimeZone (DST)
    v_start   double precision := extract(epoch FROM v_from);
    v_span    double precision := extract(epoch FROM v_to) - extract(epoch FROM v_from);
    v_done    integer := 0;
    v_rows    integer;
    v_lo      double precision;
    v_hi      double precision;
    v_started timestamptz := clock_timestamp();
BEGIN
    IF p_rows IS NULL OR p_rows <= 0 THEN
        RAISE EXCEPTION 'p_rows must be positive';
    END IF;
    IF p_clients IS NULL OR p_clients <= 0 OR p_batch_size IS NULL OR p_batch_size <= 0 THEN
        RAISE EXCEPTION 'p_clients and p_batch_size must be positive';
    END IF;
    IF p_pending_share IS NULL OR p_pending_share NOT BETWEEN 0 AND 1 THEN
        RAISE EXCEPTION 'p_pending_share must be within [0, 1]';
    END IF;
    IF v_from >= v_to OR NOT isfinite(v_from) OR NOT isfinite(v_to) THEN
        RAISE EXCEPTION 'empty period [%, %)', v_from, v_to;
    END IF;

    PERFORM t1_ensure_partitions(v_from, v_to - interval '1 microsecond');
    IF p_commit THEN COMMIT; END IF;

    WHILE v_done < p_rows LOOP
        v_rows := least(p_batch_size, p_rows - v_done);
        v_lo   := v_start + v_span * v_done / p_rows;
        v_hi   := v_start + v_span * (v_done + v_rows) / p_rows;

        INSERT INTO t1 (op_date, amount, state, operation_guid, message)
        SELECT g.op_date,
               round((1 + random() * 9999)::numeric, 2),
               CASE WHEN random() < p_pending_share THEN 0 ELSE 1 END,
               t1_uuidv7_at(g.op_date),
               t1_make_message(g.client_id,
                               1 + floor(random() * 3)::integer,
                               CASE WHEN random() < 0.5 THEN 'online' ELSE 'offline' END)
          FROM (SELECT to_timestamp(v_lo + random() * (v_hi - v_lo))  AS op_date,
                       1 + floor(random() * p_clients)::bigint         AS client_id
                  FROM generate_series(1, v_rows)) AS g
         ORDER BY g.op_date;

        v_done := v_done + v_rows;
        IF p_commit THEN COMMIT; END IF;
        RAISE NOTICE 't1_generate_data: %/% rows', v_done, p_rows;
    END LOOP;

    -- confirmed rows were inserted directly, so the totals view is refreshed explicitly
    PERFORM t1_refresh_client_totals();
    ANALYZE t1;
    RAISE NOTICE 't1_generate_data: % rows in %', p_rows, clock_timestamp() - v_started;
END
$$;

REVOKE EXECUTE ON PROCEDURE t1_generate_data(integer, timestamptz, timestamptz, integer, numeric, integer, boolean) FROM PUBLIC;


-- Requirement 4: one new operation with state = 0; missing arguments are random. Duplicate guid -> 23505.
CREATE FUNCTION t1_add_operation(
    p_amount         numeric     DEFAULT NULL,
    p_client_id      bigint      DEFAULT NULL,
    p_operation_type text        DEFAULT NULL,
    p_account_number text        DEFAULT NULL,
    p_operation_guid uuid        DEFAULT NULL,
    p_op_date        timestamptz DEFAULT NULL)
RETURNS t1
LANGUAGE plpgsql
AS $$
DECLARE
    v_op_date   timestamptz := coalesce(p_op_date, clock_timestamp());
    v_client_id bigint := coalesce(p_client_id, 1 + floor(random() * 1000)::bigint);
    v_message   jsonb  := t1_make_message(v_client_id,
                                          1 + floor(random() * 3)::integer,
                                          coalesce(p_operation_type, CASE WHEN random() < 0.5 THEN 'online' ELSE 'offline' END));
    v_row       t1;
BEGIN
    IF p_account_number IS NOT NULL THEN
        v_message := jsonb_set(v_message, '{account_number}', to_jsonb(p_account_number));
    END IF;

    INSERT INTO t1 (op_date, amount, state, operation_guid, message)
    VALUES (v_op_date,
            coalesce(p_amount, round((1 + random() * 9999)::numeric, 2)),
            0,
            coalesce(p_operation_guid, t1_uuidv7_at(v_op_date)),
            v_message)
    RETURNING * INTO v_row;

    RETURN v_row;
END
$$;


-- Requirement 5: state 0 -> 1 for even ids if the second of p_at (default: now, UTC) is even, odd ids otherwise.
-- The table lock is taken first, so the second is read when the update can really run; rows locked by
-- other transactions are skipped (the next tick of that parity takes them).
CREATE FUNCTION t1_confirm_operations(p_at timestamptz DEFAULT NULL)
RETURNS TABLE (second_of_minute integer, parity text, confirmed_count bigint)
LANGUAGE plpgsql
AS $$
DECLARE
    v_second integer;
    v_parity integer;
    v_count  bigint;
BEGIN
    IF p_at IS NOT NULL AND NOT isfinite(p_at) THEN
        RAISE EXCEPTION 'p_at must be finite';
    END IF;

    LOCK TABLE t1 IN ROW EXCLUSIVE MODE;
    v_second := floor(extract(second FROM coalesce(p_at, clock_timestamp()) AT TIME ZONE 'UTC'))::integer;
    v_parity := v_second % 2;

    UPDATE t1 SET state = 1
     WHERE (id, op_date) IN (SELECT p.id, p.op_date
                               FROM t1 p
                              WHERE p.state = 0 AND (p.id & 1) = v_parity
                                FOR UPDATE SKIP LOCKED);
    GET DIAGNOSTICS v_count = ROW_COUNT;

    RETURN QUERY SELECT v_second, CASE v_parity WHEN 0 THEN 'even' ELSE 'odd' END, v_count;
END
$$;
