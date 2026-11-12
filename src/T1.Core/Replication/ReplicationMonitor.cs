using NpgsqlTypes;
using T1.Core.Database;
using T1.Core.Operations;

namespace T1.Core.Replication;

public sealed record SubscriptionStatus(bool Exists, bool Enabled, int? ApplyWorkerPid, string? ReceivedLsn, DateTime? LastMessageReceiptTime, DateTime? LatestEndTime);

public sealed record SlotStatus(bool Exists, bool Active, string? ConfirmedFlushLsn, long? LagBytes);

public sealed record ReplicationStatus(
    string Publication,
    string Subscription,
    SubscriptionStatus Subscriber,
    SlotStatus PublisherSlot,
    TableSnapshot Primary,
    TableSnapshot Replica,
    bool DataInSync);

/// <summary>pg_stat_subscription on the replica, the slot on the primary, and t1 compared on both instances.</summary>
public sealed class ReplicationMonitor(T1DataSources db, ReplicationOptions options)
{
    public async Task<ReplicationStatus> GetStatusAsync(CancellationToken ct = default)
    {
        var subscriber = await GetSubscriptionStatusAsync(ct);
        var slot = await GetSlotStatusAsync(ct);
        var primary = await OperationsRepository.GetSnapshotAsync(db.Primary, ct);
        var replica = await OperationsRepository.GetSnapshotAsync(db.Replica, ct);

        var inSync = primary.Rows == replica.Rows
                     && primary.PendingRows == replica.PendingRows
                     && primary.MaxId == replica.MaxId
                     && primary.TotalAmount == replica.TotalAmount;

        return new ReplicationStatus(options.PublicationName, options.SubscriptionName, subscriber, slot, primary, replica, inSync);
    }

    public async Task<SubscriptionStatus> GetSubscriptionStatusAsync(CancellationToken ct = default)
    {
        await using var command = db.Replica.CreateCommand("""
            SELECT s.subenabled, st.pid, st.received_lsn::text, st.last_msg_receipt_time, st.latest_end_time
              FROM pg_subscription s
              LEFT JOIN pg_stat_subscription st ON st.subid = s.oid AND st.worker_type = 'apply'
             WHERE s.subname = @sub
               AND s.subdbid = (SELECT oid FROM pg_database WHERE datname = current_database())
            """);
        command.Parameters.Add(Sql.Param("sub", NpgsqlDbType.Text, options.SubscriptionName));
        await using var reader = await command.ExecuteReaderAsync(ct);
        if (!await reader.ReadAsync(ct))
        {
            return new SubscriptionStatus(false, false, null, null, null, null);
        }

        return new SubscriptionStatus(
            true,
            reader.GetBoolean(0),
            reader.IsDBNull(1) ? null : reader.GetInt32(1),
            reader.IsDBNull(2) ? null : reader.GetString(2),
            reader.IsDBNull(3) ? null : reader.GetFieldValue<DateTime>(3),
            reader.IsDBNull(4) ? null : reader.GetFieldValue<DateTime>(4));
    }

    private async Task<SlotStatus> GetSlotStatusAsync(CancellationToken ct)
    {
        await using var command = db.Primary.CreateCommand("""
            SELECT active, confirmed_flush_lsn::text, pg_wal_lsn_diff(pg_current_wal_lsn(), confirmed_flush_lsn)::bigint
              FROM pg_replication_slots
             WHERE slot_name = @slot
            """);
        command.Parameters.Add(Sql.Param("slot", NpgsqlDbType.Text, options.SubscriptionName));
        await using var reader = await command.ExecuteReaderAsync(ct);
        if (!await reader.ReadAsync(ct))
        {
            return new SlotStatus(false, false, null, null);
        }

        return new SlotStatus(
            true,
            reader.GetBoolean(0),
            reader.IsDBNull(1) ? null : reader.GetString(1),
            reader.IsDBNull(2) ? null : reader.GetInt64(2));
    }
}
