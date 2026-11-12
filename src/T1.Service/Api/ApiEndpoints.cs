using Npgsql;
using T1.Core;
using T1.Core.Operations;
using T1.Core.Partitions;
using T1.Core.Replication;
using T1.Core.Scheduling;
using T1.Service.Hosting;

namespace T1.Service.Api;

public sealed record CreateOperationRequest(
    decimal? Amount,
    long? ClientId,
    string? OperationType,
    string? AccountNumber,
    Guid? OperationGuid,
    DateTime? OpDate);

/// <summary>HTTP API to watch the system; examples in T1.Service.http.</summary>
public static class ApiEndpoints
{
    public static WebApplication MapT1Api(this WebApplication app)
    {
        app.MapGet("/", () => Results.Redirect("/api"));

        // live checks with short timeouts; 503 only when the primary (needed by the jobs) is down
        app.MapGet("/health", async (T1DataSources db, ReplicationMonitor monitor, LeaderElection leader, ReplicaStatus replica, CancellationToken ct) =>
        {
            var primary = await ProbeAsync(async token => { await PingAsync(db.Primary, token); return "up"; }, ct);
            var subscription = await ProbeAsync(async token =>
            {
                var status = await monitor.GetSubscriptionStatusAsync(token);
                return !status.Exists ? "missing" : status.ApplyWorkerPid is null ? "not streaming" : "streaming";
            }, ct);

            var body = new { primary, replica = subscription, schedulerLeader = leader.IsLeader, replicaSetup = replica.State, replicaError = replica.Error };
            return primary is "up" ? Results.Ok(body) : Results.Json(body, statusCode: StatusCodes.Status503ServiceUnavailable);
        });

        var api = app.MapGroup("/api");

        api.MapGet("/", () => new
        {
            endpoints = new[]
            {
                "GET  /api/stats",
                "GET  /api/operations?limit=20",
                "GET  /api/operations/{guid}",
                "POST /api/operations",
                "POST /api/operations/confirm",
                "GET  /api/client-totals?clientId=&operationType=&limit=",
                "GET  /api/replication",
            },
        });

        api.MapGet("/stats", async (T1DataSources db, CancellationToken ct) => new
        {
            primary = await OperationsRepository.GetSnapshotAsync(db.Primary, ct),
            primaryPartitions = await PartitionManager.ListAsync(db.Primary, ct),
            replica = await ProbeAsync<object>(async token => await OperationsRepository.GetSnapshotAsync(db.Replica, token), ct),
            replicaPartitions = await ProbeAsync<object>(async token => await PartitionManager.ListAsync(db.Replica, token), ct),
        });

        api.MapGet("/operations", async (int? limit, OperationsRepository operations, CancellationToken ct) =>
            await operations.GetLatestAsync(Math.Clamp(limit ?? 20, 1, 1000), ct));

        api.MapGet("/operations/{operationGuid:guid}", async (Guid operationGuid, OperationsRepository operations, CancellationToken ct) =>
            await operations.FindByGuidAsync(operationGuid, ct) is { } operation ? Results.Ok(operation) : Results.NotFound());

        api.MapPost("/operations", async (CreateOperationRequest request, OperationsRepository operations, CancellationToken ct) =>
        {
            if (request.Amount is { } amount && (amount <= 0 || decimal.Round(amount, 2) != amount))
            {
                return Results.ValidationProblem(new Dictionary<string, string[]> { ["amount"] = ["must be positive with at most 2 decimals"] });
            }

            if (request.ClientId is <= 0)
            {
                return Results.ValidationProblem(new Dictionary<string, string[]> { ["clientId"] = ["must be positive"] });
            }

            if (request.OperationType is not null && !OperationTypes.IsValid(request.OperationType))
            {
                return Results.ValidationProblem(new Dictionary<string, string[]> { ["operationType"] = ["must be 'online' or 'offline'"] });
            }

            // a date without an offset is UTC, whatever the time zone of the host
            var opDate = request.OpDate switch
            {
                null => (DateTime?)null,
                { Kind: DateTimeKind.Unspecified } date => DateTime.SpecifyKind(date, DateTimeKind.Utc),
                { } date => date.ToUniversalTime(),
            };

            try
            {
                var operation = await operations.AddOperationAsync(
                    new NewOperation(request.Amount, request.ClientId, request.OperationType, request.AccountNumber, request.OperationGuid, opDate), ct);
                return Results.Created($"/api/operations/{operation.OperationGuid}", operation);
            }
            catch (PostgresException e) when (e.SqlState == PostgresErrorCodes.UniqueViolation)
            {
                return Results.Problem(title: "Duplicate operation_guid", detail: e.MessageText, statusCode: StatusCodes.Status409Conflict);
            }
            catch (PostgresException e) when (e.SqlState is PostgresErrorCodes.CheckViolation or PostgresErrorCodes.NumericValueOutOfRange
                                                  or PostgresErrorCodes.InvalidTextRepresentation or PostgresErrorCodes.InvalidParameterValue)
            {
                return Results.Problem(title: "Invalid operation", detail: e.MessageText, statusCode: StatusCodes.Status400BadRequest);
            }
        });

        api.MapPost("/operations/confirm", async (OperationsRepository operations, CancellationToken ct) =>
            await operations.ConfirmOperationsAsync(null, ct));

        api.MapGet("/client-totals", async (long? clientId, string? operationType, int? limit, OperationsRepository operations, CancellationToken ct) => new
        {
            summary = await operations.GetClientTotalsSummaryAsync(ct),
            rows = await operations.GetClientTotalsAsync(clientId, operationType, Math.Clamp(limit ?? 50, 1, 10_000), ct),
        });

        api.MapGet("/replication", async (ReplicationMonitor monitor, CancellationToken ct) =>
        {
            try
            {
                return Results.Ok(await WithTimeout(monitor.GetStatusAsync, ct));
            }
            catch (Exception e) when (e is NpgsqlException or OperationCanceledException && !ct.IsCancellationRequested)
            {
                return Results.Problem(title: "Replication status is unavailable", detail: e.Message, statusCode: StatusCodes.Status503ServiceUnavailable);
            }
        });

        return app;
    }

    private static async Task PingAsync(NpgsqlDataSource dataSource, CancellationToken ct)
    {
        await using var command = dataSource.CreateCommand("SELECT 1");
        await command.ExecuteScalarAsync(ct);
    }

    /// <summary>Runs a query against an instance that may be down or hanging: 5 s at most, errors become a message.</summary>
    private static async Task<T> WithTimeout<T>(Func<CancellationToken, Task<T>> query, CancellationToken ct)
    {
        using var timeout = CancellationTokenSource.CreateLinkedTokenSource(ct);
        timeout.CancelAfter(TimeSpan.FromSeconds(5));
        return await query(timeout.Token);
    }

    private static async Task<object> ProbeAsync<T>(Func<CancellationToken, Task<T>> query, CancellationToken ct)
    {
        try
        {
            return (object?)await WithTimeout(query, ct) ?? "null";
        }
        catch (Exception e) when (e is NpgsqlException or OperationCanceledException or TimeoutException && !ct.IsCancellationRequested)
        {
            return new { error = e.Message };
        }
    }
}
