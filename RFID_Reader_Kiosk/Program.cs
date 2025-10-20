// .NET 8 Console App — RFID -> SQL Server (Dapper) -> print CarteSLV by RfidHex
// Build: dotnet build
// Run  : dotnet run

using System.Buffers;
using System.Net.Sockets;
using System.Text;
using System.Text.RegularExpressions;
using Dapper;
using Microsoft.Data.SqlClient;
using Microsoft.Extensions.Configuration;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Hosting;
using Microsoft.Extensions.Logging;
using Microsoft.Extensions.Options;

#region Options

public sealed class RfidOptions
{
    public string Host { get; set; } = "10.116.136.27";
    public int Port { get; set; } = 4001;
    public int ReconnectDelayMs { get; set; } = 2000;
    public int ReadBufferBytes { get; set; } = 4096;
    public int DebounceSeconds { get; set; } = 5;
    /// <summary>
    /// Optional custom line terminator regex (else CR/LF split).
    /// </summary>
    public string? LineTerminatorRegex { get; set; }
}

public sealed class DbOptions
{
    public string ConnectionString { get; set; } = default!;
    /// <summary>
    /// Table name – your codebase refers to Ecare_ClientEquipements.
    /// </summary>
    public string ClientEquipementsTable { get; set; } = "dbo.Ecare_ClientEquipements";
}

#endregion

#region RFID

public sealed class TagEventArgs : EventArgs
{
    public required string RawAscii { get; init; }    // what came as ASCII line post-clean
    public required string HexCanonical { get; init; } // canonical HEX we’ll use to query DB
    public required DateTime Timestamp { get; init; }
}

public sealed class RfidService : IHostedService
{
    private readonly ILogger<RfidService> _log;
    private readonly RfidOptions _opt;
    private CancellationTokenSource? _cts;
    public event EventHandler<TagEventArgs>? TagReceived;

    public RfidService(ILogger<RfidService> log, IOptions<RfidOptions> opt)
    {
        _log = log; _opt = opt.Value;
    }

    public Task StartAsync(CancellationToken cancellationToken)
    {
        _cts = CancellationTokenSource.CreateLinkedTokenSource(cancellationToken);
        _ = Task.Run(() => RunAsync(_cts.Token));
        return Task.CompletedTask;
    }

    public Task StopAsync(CancellationToken cancellationToken)
    {
        try { _cts?.Cancel(); } catch { /* ignore */ }
        return Task.CompletedTask;
    }

    private async Task RunAsync(CancellationToken ct)
    {
        var lastSeen = new Dictionary<string, DateTime>(StringComparer.OrdinalIgnoreCase);
        var debWindow = TimeSpan.FromSeconds(_opt.DebounceSeconds);

        while (!ct.IsCancellationRequested)
        {
            try
            {
                using var tcp = new TcpClient();
                var connectTask = tcp.ConnectAsync(_opt.Host, _opt.Port);
                var winner = await Task.WhenAny(connectTask, Task.Delay(_opt.ReconnectDelayMs, ct));
                if (winner != connectTask) throw new TimeoutException("RFID connect timeout");
                await connectTask;

                using var stream = tcp.GetStream();
                var buffer = ArrayPool<byte>.Shared.Rent(_opt.ReadBufferBytes);
                var sb = new StringBuilder();

                _log.LogInformation("RFID connected to {host}:{port}", _opt.Host, _opt.Port);

                try
                {
                    while (!ct.IsCancellationRequested)
                    {
                        if (!stream.DataAvailable) { await Task.Delay(5, ct); continue; }

                        int n = await stream.ReadAsync(buffer.AsMemory(0, _opt.ReadBufferBytes), ct);
                        if (n <= 0) throw new IOException("RFID remote closed");

                        var chunk = Encoding.ASCII.GetString(buffer, 0, n);
                        sb.Append(chunk);

                        foreach (var line in SplitLines(sb, _opt.LineTerminatorRegex))
                        {
                            var clean = StripControlChars(line).Trim();
                            if (string.IsNullOrEmpty(clean)) continue;

                            var hexCanonical = ToCanonicalHex(clean);
                            var now = DateTime.Now;

                            if (hexCanonical.Length == 0) continue;
                            if (lastSeen.TryGetValue(hexCanonical, out var t) && now - t < debWindow) continue;
                            lastSeen[hexCanonical] = now;

                            _log.LogInformation("[RFID] Raw='{raw}'  HEX={hex}", clean, hexCanonical);
                            TagReceived?.Invoke(this, new TagEventArgs
                            {
                                RawAscii = clean,
                                HexCanonical = hexCanonical,
                                Timestamp = now
                            });
                        }
                    }
                }
                finally { ArrayPool<byte>.Shared.Return(buffer); }
            }
            catch (OperationCanceledException) { break; }
            catch (Exception ex)
            {
                _log.LogWarning(ex, "RFID loop error; retrying in {ms}ms", _opt.ReconnectDelayMs);
                try { await Task.Delay(_opt.ReconnectDelayMs, ct); } catch { /* ignore */ }
            }
        }
    }

    private static IEnumerable<string> SplitLines(StringBuilder sb, string? customTerminatorRegex)
    {
        if (string.IsNullOrEmpty(customTerminatorRegex))
        {
            var text = sb.ToString();
            var lines = text.Split(new[] { "\r\n", "\n", "\r" }, StringSplitOptions.None);
            for (int i = 0; i < lines.Length - 1; i++) yield return lines[i];
            sb.Clear().Append(lines[^1]);
        }
        else
        {
            var rx = new Regex(customTerminatorRegex, RegexOptions.Compiled);
            string text = sb.ToString();
            int last = 0;
            foreach (Match m in rx.Matches(text))
            {
                yield return text[last..m.Index];
                last = m.Index + m.Length;
            }
            sb.Clear().Append(text[last..]);
        }
    }

    private static string StripControlChars(string s)
    {
        var b = new StringBuilder(s.Length);
        foreach (var ch in s) if (!char.IsControl(ch)) b.Append(ch);
        return b.ToString();
    }

    /// <summary>
    /// If the ASCII line already looks like HEX, normalize to uppercase (even length).
    /// Otherwise, convert ASCII bytes to HEX (uppercase, no dashes/spaces).
    /// </summary>
    private static string ToCanonicalHex(string ascii)
    {
        var hexLike = Regex.IsMatch(ascii, "^[0-9A-Fa-f]+$");
        if (hexLike && ascii.Length % 2 == 0)
            return ascii.ToUpperInvariant();

        // Convert ASCII characters to their byte values and hex-encode that.
        var bytes = Encoding.ASCII.GetBytes(ascii);
        var sb = new StringBuilder(bytes.Length * 2);
        foreach (var b in bytes) sb.Append(b.ToString("X2"));
        return sb.ToString();
    }
}

#endregion

#region Repository

public interface IClientEquipementRepository
{
    Task<string?> GetCarteSlvByRfidHexAsync(string hexCanonical, CancellationToken ct);
}

public sealed class ClientEquipementRepository : IClientEquipementRepository
{
    private readonly ILogger<ClientEquipementRepository> _log;
    private readonly DbOptions _opt;

    public ClientEquipementRepository(ILogger<ClientEquipementRepository> log, IOptions<DbOptions> opt)
    {
        _log = log; _opt = opt.Value;
    }

    public async Task<string?> GetCarteSlvByRfidHexAsync(string hexCanonical, CancellationToken ct)
    {
        const string sqlTemplate = """
            SELECT TOP(1) CarteSLV
            FROM {TABLE}
            WHERE RfidHex = @hex
            """;

        var sql = sqlTemplate.Replace("{TABLE}", _opt.ClientEquipementsTable);
        using var conn = new SqlConnection(_opt.ConnectionString);
        await conn.OpenAsync(ct);

        var slv = await conn.QueryFirstOrDefaultAsync<string?>(new CommandDefinition(
            sql, new { hex = hexCanonical }, cancellationToken: ct));

        if (slv is null)
            _log.LogWarning("No match for RfidHex={hex}", hexCanonical);
        else
            _log.LogInformation("Match: RfidHex={hex} -> CarteSLV={slv}", hexCanonical, slv);

        return slv;
    }
}

#endregion

#region Resolver (wires RFID to DB)

public sealed class RfidResolverService : IHostedService
{
    private readonly ILogger<RfidResolverService> _log;
    private readonly RfidService _rfid;
    private readonly IClientEquipementRepository _repo;

    public RfidResolverService(
        ILogger<RfidResolverService> log,
        RfidService rfid,
        IClientEquipementRepository repo)
    { _log = log; _rfid = rfid; _repo = repo; }

    public Task StartAsync(CancellationToken cancellationToken)
    {
        _rfid.TagReceived += OnTag;
        _log.LogInformation("Ready. Waiting for RFID tags…");
        return Task.CompletedTask;
    }

    public Task StopAsync(CancellationToken cancellationToken)
    {
        _rfid.TagReceived -= OnTag;
        return Task.CompletedTask;
    }

    private async void OnTag(object? sender, TagEventArgs e)
    {
        try
        {
            var slv = await _repo.GetCarteSlvByRfidHexAsync(e.HexCanonical, CancellationToken.None);

            if (slv is null)
            {
                Console.WriteLine($"[{e.Timestamp:HH:mm:ss}] HEX={e.HexCanonical} -> NOT FOUND");
            }
            else
            {
                Console.WriteLine($"[{e.Timestamp:HH:mm:ss}] HEX={e.HexCanonical} -> CarteSLV={slv}");
            }
        }
        catch (Exception ex)
        {
            _log.LogError(ex, "Lookup failed for HEX={hex}", e.HexCanonical);
        }
    }
}

#endregion

#region Program

public class Program
{
    public static async Task Main(string[] args)
    {
        using var host = Host.CreateDefaultBuilder(args)
            .ConfigureAppConfiguration(cfg =>
            {
                cfg.AddJsonFile("appsettings.json", optional: true, reloadOnChange: true)
                   .AddEnvironmentVariables();
            })
            .ConfigureServices((ctx, services) =>
            {
                // Bind options
                services.Configure<RfidOptions>(ctx.Configuration.GetSection("Rfid"));
                services.Configure<DbOptions>(opt =>
                {
                    // ConnectionStrings:SqlServer as provided
                    opt.ConnectionString = ctx.Configuration.GetConnectionString("SqlServer")
                                         ?? throw new InvalidOperationException("ConnectionStrings:SqlServer missing");
                    // Allow override via config: Db:ClientEquipementsTable
                    var table = ctx.Configuration["Db:ClientEquipementsTable"];
                    if (!string.IsNullOrWhiteSpace(table)) opt.ClientEquipementsTable = table!;
                });

                // Services
                services.AddSingleton<RfidService>();
                services.AddSingleton<IClientEquipementRepository, ClientEquipementRepository>();
                services.AddSingleton<RfidResolverService>();

                // Hosted services
                services.AddHostedService(sp => sp.GetRequiredService<RfidService>());
                services.AddHostedService(sp => sp.GetRequiredService<RfidResolverService>());
            })
            .ConfigureLogging(logging =>
            {
                logging.ClearProviders();
                logging.AddSimpleConsole(o =>
                {
                    o.SingleLine = true;
                    o.TimestampFormat = "HH:mm:ss ";
                });
                logging.SetMinimumLevel(LogLevel.Information);
            })
            .Build();

        Console.WriteLine("RFID → SQL lookup (CarteSLV). Press Ctrl+C to exit.");
        await host.RunAsync();
    }
}

#endregion
