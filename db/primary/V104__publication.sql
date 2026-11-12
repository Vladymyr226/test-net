-- Requirement 7, publisher side (needs wal_level = logical). Changes of every partition, including future
-- ones, are published as changes of t1, so the subscriber routes them into its own partitions.
-- The replication role and the subscription are created by the service (ReplicationConfigurator).
CREATE PUBLICATION t1_pub
    FOR TABLE t1
    WITH (publish = 'insert, update, delete, truncate', publish_via_partition_root = true);
