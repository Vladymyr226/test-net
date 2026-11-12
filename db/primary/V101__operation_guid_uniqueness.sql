-- Requirement 3: operation_guid is unique across the whole partitioned table.
-- PostgreSQL has no global indexes on partitioned tables: UNIQUE (operation_guid) is rejected and
-- UNIQUE (operation_guid, op_date) would allow the same guid on another date. So the guid is the
-- primary key of a non-partitioned registry kept in sync by row triggers (cloned to every partition).
CREATE TABLE t1_operation_guid (
    operation_guid uuid        NOT NULL,
    t1_id          bigint      NOT NULL,
    op_date        timestamptz NOT NULL,
    CONSTRAINT t1_operation_guid_pkey PRIMARY KEY (operation_guid)
);

-- AFTER INSERT: only rows that were really inserted are registered (not the ones skipped by ON CONFLICT).
-- A duplicate fails on the registry's primary key with 23505 (SERIALIZABLE may report 40001 instead).
CREATE FUNCTION t1_operation_guid_on_insert()
RETURNS trigger
LANGUAGE plpgsql
SECURITY DEFINER
SET search_path = public, pg_temp
AS $$
BEGIN
    INSERT INTO t1_operation_guid (operation_guid, t1_id, op_date)
    VALUES (NEW.operation_guid, NEW.id, NEW.op_date);
    RETURN NULL;
END
$$;

-- id and op_date identify the row (op_date is the partition key), so they are immutable; a new guid re-keys the registry.
CREATE FUNCTION t1_operation_guid_on_update()
RETURNS trigger
LANGUAGE plpgsql
SECURITY DEFINER
SET search_path = public, pg_temp
AS $$
BEGIN
    IF NEW.id IS DISTINCT FROM OLD.id OR NEW.op_date IS DISTINCT FROM OLD.op_date THEN
        RAISE EXCEPTION 't1.id and t1.op_date are immutable (id=%)', OLD.id
            USING ERRCODE = 'feature_not_supported',
                  HINT    = 'Delete the operation and insert a new one instead.';
    END IF;

    IF NEW.operation_guid IS DISTINCT FROM OLD.operation_guid THEN
        UPDATE t1_operation_guid SET operation_guid = NEW.operation_guid WHERE operation_guid = OLD.operation_guid;
    END IF;
    RETURN NEW;
END
$$;

CREATE FUNCTION t1_operation_guid_on_delete()
RETURNS trigger
LANGUAGE plpgsql
SECURITY DEFINER
SET search_path = public, pg_temp
AS $$
BEGIN
    DELETE FROM t1_operation_guid WHERE operation_guid = OLD.operation_guid AND t1_id = OLD.id;
    RETURN NULL;
END
$$;

CREATE FUNCTION t1_operation_guid_on_truncate()
RETURNS trigger
LANGUAGE plpgsql
SECURITY DEFINER
SET search_path = public, pg_temp
AS $$
BEGIN
    TRUNCATE t1_operation_guid;
    RETURN NULL;
END
$$;

CREATE TRIGGER t1_operation_guid_insert
    AFTER INSERT ON t1
    FOR EACH ROW EXECUTE FUNCTION t1_operation_guid_on_insert();

CREATE TRIGGER t1_operation_guid_update
    BEFORE UPDATE OF id, op_date, operation_guid ON t1
    FOR EACH ROW
    WHEN (NEW.id IS DISTINCT FROM OLD.id
          OR NEW.op_date IS DISTINCT FROM OLD.op_date
          OR NEW.operation_guid IS DISTINCT FROM OLD.operation_guid)
    EXECUTE FUNCTION t1_operation_guid_on_update();

CREATE TRIGGER t1_operation_guid_delete
    AFTER DELETE ON t1
    FOR EACH ROW EXECUTE FUNCTION t1_operation_guid_on_delete();

CREATE TRIGGER t1_operation_guid_truncate
    AFTER TRUNCATE ON t1
    FOR EACH STATEMENT EXECUTE FUNCTION t1_operation_guid_on_truncate();


-- Repairs the registry after operations that bypass row triggers (DROP/DETACH/ATTACH or TRUNCATE of a
-- single partition): removes entries without a row and registers rows without an entry.
CREATE FUNCTION t1_operation_guid_resync(OUT removed bigint, OUT added bigint)
LANGUAGE plpgsql
AS $$
DECLARE
    v_duplicate uuid;
BEGIN
    SELECT operation_guid INTO v_duplicate FROM t1 GROUP BY operation_guid HAVING count(*) > 1 LIMIT 1;
    IF FOUND THEN
        RAISE EXCEPTION 't1 contains duplicate operation_guid %', v_duplicate USING ERRCODE = 'unique_violation';
    END IF;

    WITH gone AS (
        DELETE FROM t1_operation_guid g
         WHERE NOT EXISTS (SELECT 1 FROM t1
                            WHERE t1.id = g.t1_id AND t1.op_date = g.op_date AND t1.operation_guid = g.operation_guid)
        RETURNING 1)
    SELECT count(*) INTO removed FROM gone;

    WITH missing AS (
        INSERT INTO t1_operation_guid (operation_guid, t1_id, op_date)
        SELECT t.operation_guid, t.id, t.op_date
          FROM t1 t
         WHERE NOT EXISTS (SELECT 1 FROM t1_operation_guid g WHERE g.operation_guid = t.operation_guid)
        RETURNING 1)
    SELECT count(*) INTO added FROM missing;
END
$$;

-- Trigger functions run as their owner; nobody else may call them (e.g. from a trigger on a temp table).
REVOKE EXECUTE ON FUNCTION t1_operation_guid_on_insert(), t1_operation_guid_on_update(),
                           t1_operation_guid_on_delete(), t1_operation_guid_on_truncate(),
                           t1_operation_guid_resync() FROM PUBLIC;
