using System.Globalization;
using System.Security.Cryptography;
using System.Text;
using System.Text.RegularExpressions;
using Microsoft.Extensions.Logging;
using Npgsql;
using NpgsqlTypes;

namespace T1.Core.Database;

public enum DatabaseRole
{
    /// <summary>db/common + db/primary</summary>
    Primary,

    /// <summary>db/common (+ db/replica, if any)</summary>
    Replica,
}

public sealed record MigrationScript(int Version, string Name, string Sql, string Checksum);

/// <summary>
/// Forward-only migrator for the embedded db/*.sql scripts: one transaction per script, history in
/// schema_migrations, an advisory lock against concurrent runs.
/// </summary>
public sealed partial class DbMigrator(ILogger<DbMigrator> logger)
{
    private const long LockKey = 7_310_001;

    public static IReadOnlyList<MigrationScript> GetScripts(DatabaseRole role)
    {
        string[] folders = role == DatabaseRole.Primary ? ["common", "primary"] : ["common", "replica"];
        var assembly = typeof(DbMigrator).Assembly;

        var scripts = new List<MigrationScript>();
        foreach (var resource in assembly.GetManifestResourceNames())
        {
            var parts = resource.Replace('\\', '/').Split('/');
            if (parts.Length != 3 || parts[0] != "db" || !folders.Contains(parts[1]))
            {
                continue;
            }

            var match = ScriptNameRegex().Match(parts[2]);
            if (!match.Success)
            {
                throw new InvalidOperationException($"Migration '{resource}' does not match V<version>__<name>.sql");
            }

            using var stream = assembly.GetManifestResourceStream(resource)!;
            using var reader = new StreamReader(stream, Encoding.UTF8);
            var sql = reader.ReadToEnd().Replace("\r\n", "\n", StringComparison.Ordinal);
            var checksum = Convert.ToHexStringLower(SHA256.HashData(Encoding.UTF8.GetBytes(sql)));
            scripts.Add(new MigrationScript(int.Parse(match.Groups[1].Value, CultureInfo.InvariantCulture), $"{parts[1]}/{parts[2]}", sql, checksum));
        }

        var duplicate = scripts.GroupBy(s => s.Version).FirstOrDefault(g => g.Count() > 1);
        if (duplicate is not null)
        {
            throw new InvalidOperationException($"Duplicate migration version {duplicate.Key}: {string.Join(", ", duplicate.Select(s => s.Name))}");
        }

        return scripts.OrderBy(s => s.Version).ToList();
    }

    /// <returns>Number of scripts applied by this call.</returns>
    public async Task<int> MigrateAsync(NpgsqlDataSource dataSource, DatabaseRole role, CancellationToken ct = default)
    {
        var scripts = GetScripts(role);

        await using var connection = await dataSource.OpenConnectionAsync(ct);
        await using var _ = await Sql.AdvisoryLockAsync(connection, LockKey, ct);

        await Sql.ExecuteAsync(connection, """
            CREATE TABLE IF NOT EXISTS schema_migrations (
                version    integer     PRIMARY KEY,
                name       text        NOT NULL,
                checksum   text        NOT NULL,
                applied_at timestamptz NOT NULL DEFAULT now()
            )
            """, ct);

        var applied = new Dictionary<int, string>();
        await using (var command = new NpgsqlCommand("SELECT version, checksum FROM schema_migrations", connection))
        await using (var reader = await command.ExecuteReaderAsync(ct))
        {
            while (await reader.ReadAsync(ct))
            {
                applied[reader.GetInt32(0)] = reader.GetString(1);
            }
        }

        var count = 0;
        foreach (var script in scripts)
        {
            if (applied.TryGetValue(script.Version, out var checksum))
            {
                if (checksum != script.Checksum)
                {
                    throw new InvalidOperationException(
                        $"Migration {script.Name} was modified after it had been applied to the {role} database. " +
                        "Add a new migration instead, or recreate the databases (docker compose down -v).");
                }

                continue;
            }

            await using var transaction = await connection.BeginTransactionAsync(ct);
            await using (var command = new NpgsqlCommand(script.Sql, connection, transaction) { CommandTimeout = 0 })
            {
                await command.ExecuteNonQueryAsync(ct);
            }

            await using (var command = new NpgsqlCommand("INSERT INTO schema_migrations (version, name, checksum) VALUES (@version, @name, @checksum)", connection, transaction))
            {
                command.Parameters.Add(Sql.Param("version", NpgsqlDbType.Integer, script.Version));
                command.Parameters.Add(Sql.Param("name", NpgsqlDbType.Text, script.Name));
                command.Parameters.Add(Sql.Param("checksum", NpgsqlDbType.Text, script.Checksum));
                await command.ExecuteNonQueryAsync(ct);
            }

            await transaction.CommitAsync(ct);
            logger.LogInformation("Applied migration {Migration} to the {Role} database", script.Name, role);
            count++;
        }

        return count;
    }

    [GeneratedRegex(@"^V(\d+)__[A-Za-z0-9_]+\.sql$")]
    private static partial Regex ScriptNameRegex();
}
