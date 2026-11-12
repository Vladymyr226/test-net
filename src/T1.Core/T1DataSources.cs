using Microsoft.Extensions.Logging;
using Npgsql;

namespace T1.Core;

/// <summary>Connection pools of the primary (owns T1, publisher) and the replica (subscriber).</summary>
public sealed class T1DataSources : IAsyncDisposable
{
    public T1DataSources(NpgsqlDataSource primary, NpgsqlDataSource replica, NpgsqlDataSource primaryUnpooled)
    {
        Primary = primary;
        Replica = replica;
        PrimaryUnpooled = primaryUnpooled;
    }

    public NpgsqlDataSource Primary { get; }

    public NpgsqlDataSource Replica { get; }

    /// <summary>Closing a connection really ends its session (and releases its session-level locks).</summary>
    public NpgsqlDataSource PrimaryUnpooled { get; }

    public static T1DataSources Create(string primaryConnectionString, string replicaConnectionString, ILoggerFactory? loggerFactory = null) =>
        new(
            CreateDataSource(primaryConnectionString, loggerFactory),
            CreateDataSource(replicaConnectionString, loggerFactory),
            CreateDataSource(primaryConnectionString, loggerFactory, pooling: false));

    public static NpgsqlDataSource CreateDataSource(string connectionString, ILoggerFactory? loggerFactory = null, bool pooling = true)
    {
        var builder = new NpgsqlDataSourceBuilder(connectionString);
        var settings = builder.ConnectionStringBuilder;
        if (string.IsNullOrEmpty(settings.ApplicationName))
        {
            settings.ApplicationName = "t1-service";
        }

        // No Kerberos here; "Prefer" makes .NET try to load libgssapi_krb5 (absent in slim images) on every connect.
        if (!connectionString.Contains("GSS", StringComparison.OrdinalIgnoreCase))
        {
            settings.GssEncryptionMode = GssEncryptionMode.Disable;
        }

        if (!pooling)
        {
            settings.Pooling = false;
        }

        if (loggerFactory is not null)
        {
            builder.UseLoggerFactory(loggerFactory);
        }

        return builder.Build();
    }

    public async ValueTask DisposeAsync()
    {
        await Primary.DisposeAsync();
        await Replica.DisposeAsync();
        await PrimaryUnpooled.DisposeAsync();
    }
}
