using System.Security.Cryptography;
using System.Text;

namespace T1.Core.Scheduling;

/// <summary>
/// Deterministic UUIDv7 of a scheduler tick: every instance and every retry of the same tick gets the same guid,
/// so the unique operation_guid lets the operation of a tick be inserted only once.
/// </summary>
public static class TickGuid
{
    public static Guid For(string job, DateTimeOffset tick)
    {
        var milliseconds = tick.ToUnixTimeMilliseconds();
        var hash = SHA256.HashData(Encoding.UTF8.GetBytes($"{job}|{milliseconds}"));

        Span<byte> bytes = stackalloc byte[16];
        hash.AsSpan(0, 16).CopyTo(bytes);
        for (var i = 0; i < 6; i++)
        {
            bytes[i] = (byte)(milliseconds >> (8 * (5 - i)));   // 48-bit unix time in milliseconds
        }

        bytes[6] = (byte)(0x70 | (bytes[6] & 0x0F));   // version 7
        bytes[8] = (byte)(0x80 | (bytes[8] & 0x3F));   // RFC 9562 variant
        return new Guid(bytes, bigEndian: true);
    }
}
