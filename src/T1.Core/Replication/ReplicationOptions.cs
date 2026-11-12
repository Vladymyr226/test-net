namespace T1.Core.Replication;

public sealed class ReplicationOptions
{
    public string PublicationName { get; set; } = "t1_pub";

    /// <summary>Subscription on the replica; also the name of its slot on the primary.</summary>
    public string SubscriptionName { get; set; } = "t1_sub";

    public string User { get; set; } = "replicator";

    public string Password { get; set; } = "replicator";

    /// <summary>Address of the primary as seen from the replica instance (not from this service).</summary>
    public string PublisherHost { get; set; } = "pg-primary";

    public int PublisherPort { get; set; } = 5432;

    /// <summary>Defaults to the database of the primary connection string.</summary>
    public string? PublisherDatabase { get; set; }

    public int InitialSyncTimeoutSeconds { get; set; } = 120;

    /// <summary>How often the leader re-checks (and repairs) the replica.</summary>
    public int CheckIntervalSeconds { get; set; } = 60;
}
