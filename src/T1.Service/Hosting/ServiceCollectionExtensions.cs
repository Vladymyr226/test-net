using Microsoft.Extensions.Options;
using Quartz;
using T1.Core;
using T1.Core.Database;
using T1.Core.Operations;
using T1.Core.Partitions;
using T1.Core.Replication;
using T1.Core.Scheduling;
using T1.Core.Seeding;
using T1.Service.Jobs;

namespace T1.Service.Hosting;

public static class ServiceCollectionExtensions
{
    public static IServiceCollection AddT1Database(this IServiceCollection services)
    {
        // bound on first use, so test hosts can override the configuration
        services.AddOptions<SeedOptions>().BindConfiguration("Seed");
        services.AddOptions<PartitionOptions>().BindConfiguration("Partitions");
        services.AddOptions<ReplicationOptions>().BindConfiguration("Replication");
        services.AddSingleton(sp => sp.GetRequiredService<IOptions<SeedOptions>>().Value);
        services.AddSingleton(sp => sp.GetRequiredService<IOptions<PartitionOptions>>().Value);
        services.AddSingleton(sp => sp.GetRequiredService<IOptions<ReplicationOptions>>().Value);

        services.AddSingleton(sp =>
        {
            var configuration = sp.GetRequiredService<IConfiguration>();
            return T1DataSources.Create(
                RequiredConnectionString(configuration, "Primary"),
                RequiredConnectionString(configuration, "Replica"),
                sp.GetRequiredService<ILoggerFactory>());
        });

        services.AddSingleton<DbMigrator>();
        services.AddSingleton<DataSeeder>();
        services.AddSingleton<PartitionManager>();
        services.AddSingleton<ReplicationConfigurator>();
        services.AddSingleton<ReplicationMonitor>();
        services.AddSingleton<OperationsRepository>();
        services.AddSingleton<T1Bootstrapper>();
        services.AddSingleton<LeaderElection>();
        services.AddSingleton<ReplicaStatus>();

        services.AddHostedService<DatabaseInitializer>();   // first: the primary is ready before any job
        services.AddHostedService<LeadershipService>();
        services.AddHostedService<ReplicationSupervisor>();
        return services;
    }

    public static IServiceCollection AddT1Jobs(this IServiceCollection services, IConfiguration configuration)
    {
        var jobs = configuration.GetSection("Jobs").Get<JobsOptions>() ?? new JobsOptions();

        services.AddQuartz(quartz =>
        {
            quartz.AddCronJob<AddOperationJob>(AddOperationJob.Key, jobs.AddOperation);
            quartz.AddCronJob<ConfirmOperationsJob>(ConfirmOperationsJob.Key, jobs.ConfirmOperations);
            quartz.AddCronJob<PartitionMaintenanceJob>(PartitionMaintenanceJob.Key, jobs.PartitionMaintenance);
        });

        services.AddQuartzHostedService(options =>
        {
            options.AwaitApplicationStarted = true;
            options.WaitForJobsToComplete = false;   // on shutdown running jobs are cancelled, not awaited
        });

        return services;
    }

    private static void AddCronJob<TJob>(this IQuartzBuilder quartz, JobKey key, JobSchedule schedule)
        where TJob : class, IJob
    {
        if (!schedule.Enabled)
        {
            return;
        }

        if (!CronExpression.TryParse(schedule.Cron, out _))
        {
            throw new InvalidOperationException($"Invalid cron expression for job {key}: '{schedule.Cron}'");
        }

        quartz.AddJob<TJob>(job => job.WithIdentity(key));
        quartz.AddTrigger<TJob>(trigger => trigger
            .ForJob(key)
            .WithIdentity($"{key.Name}-trigger", key.Group)
            .WithOverlapPolicy(OverlapPolicy.Skip)              // a tick that comes while the previous run is busy is skipped
            .WithMisfireThreshold(TimeSpan.FromSeconds(1))       // a tick more than 1 s late is not run late
            .WithCronSchedule(schedule.Cron, cron => cron
                .InTimeZone(TimeZoneInfo.Utc)
                .WithMisfireInstruction(CronTriggerMisfireInstruction.DoNothing)));
    }

    private static string RequiredConnectionString(IConfiguration configuration, string name) =>
        configuration.GetConnectionString(name) is { Length: > 0 } value
            ? value
            : throw new InvalidOperationException($"ConnectionStrings:{name} is not configured");
}
