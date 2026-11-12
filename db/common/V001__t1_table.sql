-- T1: operations journal, RANGE-partitioned by month (UTC) on op_date. Applied to both instances.
CREATE TABLE t1 (
    id             bigint        GENERATED ALWAYS AS IDENTITY,
    op_date        timestamptz   NOT NULL,
    amount         numeric(18,2) NOT NULL,
    state          smallint      NOT NULL DEFAULT 0,   -- 0 = new, 1 = confirmed
    operation_guid uuid          NOT NULL,             -- globally unique, see V101
    message        jsonb         NOT NULL,

    CONSTRAINT t1_pkey PRIMARY KEY (id, op_date),      -- must contain the partition key
    CONSTRAINT t1_amount_chk CHECK (amount > 0 AND amount <> 'NaN'),   -- 'NaN' > 0 is true in PostgreSQL
    CONSTRAINT t1_state_chk CHECK (state IN (0, 1)),
    -- IS TRUE: a missing key yields NULL, which a CHECK would let through
    CONSTRAINT t1_message_chk CHECK ((
            jsonb_typeof(message) = 'object'
        AND jsonb_typeof(message -> 'account_number') = 'string'
        AND (message ->> 'account_number') ~ '^[A-Z0-9]{1,34}$'
        AND jsonb_typeof(message -> 'client_id') = 'number'
        AND (message ->> 'client_id') ~ '^[1-9][0-9]{0,17}$'
        AND (message ->> 'operation_type') IN ('online', 'offline')
    ) IS TRUE)
) PARTITION BY RANGE (op_date);

CREATE INDEX t1_op_date_idx    ON t1 (op_date);
CREATE INDEX t1_pending_id_idx ON t1 (id) WHERE state = 0;


-- Creates the missing monthly partitions t1_YYYY_MM covering [p_from, p_to]; returns how many were created.
-- Month boundaries are computed in UTC, so the result does not depend on the session TimeZone.
CREATE FUNCTION t1_ensure_partitions(p_from timestamptz, p_to timestamptz)
RETURNS integer
LANGUAGE plpgsql
AS $$
DECLARE
    v_month   timestamp;
    v_last    timestamp;
    v_name    text;
    v_created integer := 0;
BEGIN
    IF p_from IS NULL OR p_to IS NULL OR p_from > p_to OR NOT isfinite(p_from) OR NOT isfinite(p_to) THEN
        RAISE EXCEPTION 'invalid partition range [%, %]', p_from, p_to;
    END IF;

    v_month := date_trunc('month', p_from AT TIME ZONE 'UTC');
    v_last  := date_trunc('month', p_to AT TIME ZONE 'UTC');
    IF v_last >= v_month + interval '10 years' THEN
        RAISE EXCEPTION 'refusing to create more than 120 monthly partitions at once [%, %]', p_from, p_to;
    END IF;

    PERFORM pg_advisory_xact_lock(hashtext('t1_ensure_partitions'));

    WHILE v_month <= v_last LOOP
        v_name := 't1_' || to_char(v_month, 'YYYY_MM');
        IF to_regclass(v_name) IS NOT NULL
           AND NOT EXISTS (SELECT 1 FROM pg_inherits WHERE inhparent = 't1'::regclass AND inhrelid = to_regclass(v_name)) THEN
            RAISE EXCEPTION 'table % exists but is not a partition of t1 (detached?)', v_name;
        END IF;
        IF to_regclass(v_name) IS NULL THEN
            -- CREATE + ATTACH locks t1 in SHARE UPDATE EXCLUSIVE mode only: readers and writers keep going
            EXECUTE format('CREATE TABLE %I (LIKE t1 INCLUDING DEFAULTS INCLUDING CONSTRAINTS)', v_name);
            EXECUTE format('ALTER TABLE t1 ATTACH PARTITION %I FOR VALUES FROM (%L) TO (%L)',
                           v_name, v_month AT TIME ZONE 'UTC', (v_month + interval '1 month') AT TIME ZONE 'UTC');
            v_created := v_created + 1;
        END IF;
        v_month := v_month + interval '1 month';
    END LOOP;

    RETURN v_created;
END
$$;

-- Lower bound of every range partition (used to mirror partitions on the replica).
CREATE FUNCTION t1_partition_months()
RETURNS SETOF timestamptz
LANGUAGE sql
STABLE
AS $$
    SELECT (regexp_match(pg_get_expr(c.relpartbound, c.oid), 'FROM \(''([^'']+)''\)'))[1]::timestamptz
      FROM pg_inherits i
      JOIN pg_class c ON c.oid = i.inhrelid
     WHERE i.inhparent = 't1'::regclass
       AND pg_get_expr(c.relpartbound, c.oid) LIKE 'FOR VALUES FROM (''%'   -- not DEFAULT / MINVALUE
     ORDER BY 1
$$;
