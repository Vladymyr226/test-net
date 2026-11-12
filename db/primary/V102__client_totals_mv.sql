-- Requirement 6: totals of confirmed (state = 1) operations per client and operation type,
-- refreshed whenever a state changes 0 -> 1.
CREATE MATERIALIZED VIEW t1_client_totals AS
SELECT (t.message ->> 'client_id')::bigint AS client_id,
       t.message ->> 'operation_type'      AS operation_type,
       sum(t.amount)                       AS total_amount,
       count(*)                            AS operations_count
  FROM t1 t
 WHERE t.state = 1
 GROUP BY 1, 2
WITH DATA;

CREATE UNIQUE INDEX t1_client_totals_pk ON t1_client_totals (client_id, operation_type);   -- needed for CONCURRENTLY


-- The only safe way to refresh the view (the trigger, the generator and manual calls use it).
-- The advisory lock serializes refreshes; REFRESH starts after the lock is granted, so its snapshot
-- contains every confirmation committed before.
CREATE FUNCTION t1_refresh_client_totals()
RETURNS void
LANGUAGE plpgsql
SECURITY DEFINER
SET search_path = public, pg_temp
AS $$
BEGIN
    PERFORM pg_advisory_xact_lock(hashtext('t1_client_totals_refresh'));
    REFRESH MATERIALIZED VIEW CONCURRENTLY t1_client_totals;
END
$$;

-- BEFORE row trigger: every 0 -> 1 row clears the marker "refreshed in transaction X". Losing the marker
-- (RESET ALL, SET LOCAL, a rolled back savepoint) only causes an extra refresh, never a skipped one.
CREATE FUNCTION t1_client_totals_mark_dirty()
RETURNS trigger
LANGUAGE plpgsql
AS $$
BEGIN
    -- the refresh at commit must see transactions committed meanwhile, which only READ COMMITTED does
    IF current_setting('transaction_isolation') <> 'read committed' THEN
        RAISE EXCEPTION 'confirm operations (state 0 -> 1) in READ COMMITTED transactions'
            USING ERRCODE = 'feature_not_supported';
    END IF;
    PERFORM set_config('t1.client_totals_refreshed', '', true);
    RETURN NEW;
END
$$;

-- Deferred constraint trigger: refreshes once per batch (per transaction, or per statement in IMMEDIATE mode).
CREATE FUNCTION t1_client_totals_refresh()
RETURNS trigger
LANGUAGE plpgsql
SECURITY DEFINER
SET search_path = public, pg_temp
AS $$
BEGIN
    IF current_setting('t1.client_totals_refreshed', true) IS DISTINCT FROM pg_current_xact_id()::text THEN
        PERFORM set_config('t1.client_totals_refreshed', pg_current_xact_id()::text, true);
        PERFORM t1_refresh_client_totals();
    END IF;
    RETURN NULL;
END
$$;

CREATE TRIGGER t1_client_totals_dirty
    BEFORE UPDATE OF state ON t1
    FOR EACH ROW
    WHEN (OLD.state = 0 AND NEW.state = 1)
    EXECUTE FUNCTION t1_client_totals_mark_dirty();

CREATE CONSTRAINT TRIGGER t1_client_totals_refresh
    AFTER UPDATE OF state ON t1
    DEFERRABLE INITIALLY DEFERRED
    FOR EACH ROW
    WHEN (OLD.state = 0 AND NEW.state = 1)
    EXECUTE FUNCTION t1_client_totals_refresh();

REVOKE EXECUTE ON FUNCTION t1_refresh_client_totals(), t1_client_totals_refresh() FROM PUBLIC;
