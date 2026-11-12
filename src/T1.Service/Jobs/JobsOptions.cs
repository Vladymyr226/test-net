namespace T1.Service.Jobs;

/// <summary>Quartz cron: second minute hour day-of-month month day-of-week (UTC).</summary>
public sealed class JobsOptions
{
    public JobSchedule AddOperation { get; set; } = new() { Cron = "0/5 * * * * ?" };

    public JobSchedule ConfirmOperations { get; set; } = new() { Cron = "0/3 * * * * ?" };

    public JobSchedule PartitionMaintenance { get; set; } = new() { Cron = "0/10 * * * * ?" };
}

public sealed class JobSchedule
{
    public bool Enabled { get; set; } = true;

    public string Cron { get; set; } = "";
}
