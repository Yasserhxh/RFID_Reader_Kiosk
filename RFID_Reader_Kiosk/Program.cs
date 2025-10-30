// Program.cs
// .NET 8 Console App — RFID -> SQL Server (Dapper) -> Azure SignalR (slv_hub)
// Test mode: when App:TestMode = true, publish CarteSLV="4421" on a timer (no RFID/DB)
//
// Packages:
//   dotnet add package Microsoft.Azure.SignalR.Management
//   dotnet add package Microsoft.Data.SqlClient
//   dotnet add package Dapper
//   dotnet add package Microsoft.AspNetCore.App (framework reference)
//
// Run: dotnet run

using Dapper;
using Microsoft.AspNetCore.Builder;
using Microsoft.AspNetCore.Hosting;
using Microsoft.AspNetCore.Http;
using Microsoft.AspNetCore.SignalR;
using Microsoft.Azure.SignalR.Management;
using Microsoft.Data.SqlClient;
using Microsoft.Extensions.Configuration;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Hosting;
using Microsoft.Extensions.Logging;
using Microsoft.Extensions.Options;
using System;
using System.Buffers;
using System.Collections.Generic;
using System.IO;
using System.Net.Sockets;
using System.Text;
using System.Text.Json;
using System.Text.RegularExpressions;
using System.Threading;
using System.Threading.Tasks;

#region Options

public sealed class AppOptions
{
    public bool TestMode { get; set; } = false;
    public int TestIntervalMs { get; set; } = 60000;
    public int HttpPort { get; set; } = 5000; // HTTP endpoint port
}

public sealed class RfidOptions
{
    public string Host { get; set; } = "10.116.136.22";
    public int Port { get; set; } = 4001;
    public int ReconnectDelayMs { get; set; } = 2000;
    public int ReadBufferBytes { get; set; } = 4096;
    public int DebounceSeconds { get; set; } = 5;
    public string? LineTerminatorRegex { get; set; }
}

public sealed class DbOptions
{
    public string ConnectionString { get; set; } = default!;
    public string ClientEquipementsTable { get; set; } = "dbo.Ecare_ClientEquipements";
}

public sealed class SignalROptions
{
    public string ConnectionString { get; set; } = default!;
    public string HubName { get; set; } = "slv_hub";
    public string MethodName { get; set; } = "ReceiveRfid";
}

#endregion

#region Device ID Manager

public static class DeviceIdManager
{
    private static readonly string DeviceIdFile;
    private static string? _cachedDeviceId;

    static DeviceIdManager()
    {
        // Store file in the same directory as the executable/project
        var baseDirectory = AppContext.BaseDirectory;
        DeviceIdFile = Path.Combine(baseDirectory, "device.id.txt");
    }

    public static string GetOrCreateDeviceId()
    {
        if (_cachedDeviceId != null)
            return _cachedDeviceId;

        if (File.Exists(DeviceIdFile))
        {
            var existing = File.ReadAllText(DeviceIdFile).Trim();
            if (!string.IsNullOrWhiteSpace(existing))
            {
                _cachedDeviceId = existing;
                return existing;
            }
        }

        var newId = Guid.NewGuid().ToString();
        File.WriteAllText(DeviceIdFile, newId);
        _cachedDeviceId = newId;
        return newId;
    }

    public static string GetDeviceIdFilePath() => DeviceIdFile;
}

#endregion

#region RFID

public sealed class TagEventArgs : EventArgs
{
    public required string RawAscii { get; init; }
    public required string HexCanonical { get; init; }
    public required DateTime Timestamp { get; init; }
}

public sealed class RfidService : IHostedService
{
    private readonly ILogger<RfidService> _log;
    private readonly RfidOptions _opt;
    private CancellationTokenSource? _cts;
    public event EventHandler<TagEventArgs>? TagReceived;

    public RfidService(ILogger<RfidService> log, IOptions<RfidOptions> opt)
    { _log = log; _opt = opt.Value; }

    public Task StartAsync(CancellationToken cancellationToken)
    {
        _cts = CancellationTokenSource.CreateLinkedTokenSource(cancellationToken);
        _ = Task.Run(() => RunAsync(_cts.Token));
        return Task.CompletedTask;
    }

    public Task StopAsync(CancellationToken cancellationToken)
    { try { _cts?.Cancel(); } catch { } return Task.CompletedTask; }

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
                try { await Task.Delay(_opt.ReconnectDelayMs, ct); } catch { }
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

    private static string ToCanonicalHex(string ascii)
    {
        var hexLike = Regex.IsMatch(ascii, "^[0-9A-Fa-f]+$");
        if (hexLike && ascii.Length % 2 == 0)
            return ascii.ToUpperInvariant();

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
    { _log = log; _opt = opt.Value; }

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

        if (slv is null) _log.LogWarning("No match for RfidHex={hex}", hexCanonical);
        else _log.LogInformation("Match: RfidHex={hex} -> CarteSLV={slv}", hexCanonical, slv);

        return slv;
    }
}

#endregion

#region SignalR Publisher

public sealed class SignalRPublisher : IHostedService, IAsyncDisposable
{
    private readonly ILogger<SignalRPublisher> _log;
    private readonly SignalROptions _opt;
    private ServiceManager? _mgr;
    private ServiceHubContext? _hub;
    private readonly string _deviceId;

    public SignalRPublisher(ILogger<SignalRPublisher> log, IOptions<SignalROptions> opt)
    {
        _log = log;
        _opt = opt.Value;
        _deviceId = DeviceIdManager.GetOrCreateDeviceId();
        _log.LogInformation("Device ID: {deviceId}", _deviceId);
    }

    public async Task StartAsync(CancellationToken cancellationToken)
    {
        if (string.IsNullOrWhiteSpace(_opt.ConnectionString))
            throw new InvalidOperationException("SignalR:ConnectionString missing");

        _mgr = new ServiceManagerBuilder()
            .WithOptions(o => o.ConnectionString = _opt.ConnectionString)
            .BuildServiceManager();

        _hub = await _mgr.CreateHubContextAsync(_opt.HubName, cancellationToken);
        _log.LogInformation("SignalR hub context ready for {Hub}", _opt.HubName);
    }

    public Task StopAsync(CancellationToken cancellationToken) => Task.CompletedTask;

    public async ValueTask DisposeAsync()
    {
        if (_hub is not null) await _hub.DisposeAsync();
    }

    public async Task PublishRfidAsync(string? carteSlv)
    {
        if (_hub is null) return;

        var payload = new
        {
            carteSlv,
            deviceId = _deviceId,
            tsUtc = DateTime.UtcNow
        };

        try
        {
            await _hub.Clients.All.SendAsync(_opt.MethodName, payload);
            _log.LogInformation("📡 Sent via SignalR: {payload}", payload);
        }
        catch (Exception ex)
        {
            _log.LogWarning(ex, "SignalR send failed");
        }
    }
}

#endregion

#region Resolver

public sealed class RfidResolverService : IHostedService
{
    private readonly ILogger<RfidResolverService> _log;
    private readonly RfidService _rfid;
    private readonly IClientEquipementRepository _repo;
    private readonly SignalRPublisher _signalR;
    private readonly AppOptions _app;
    private CancellationTokenSource? _manualCts;

    public RfidResolverService(
        ILogger<RfidResolverService> log,
        RfidService rfid,
        IClientEquipementRepository repo,
        SignalRPublisher signalR,
        IOptions<AppOptions> app)
    {
        _log = log; _rfid = rfid; _repo = repo; _signalR = signalR; _app = app.Value;
    }

    public Task StartAsync(CancellationToken cancellationToken)
    {
        if (_app.TestMode)
        {
            _manualCts = CancellationTokenSource.CreateLinkedTokenSource(cancellationToken);
            _ = Task.Run(() => ManualInputLoopAsync(_manualCts.Token));
            _log.LogWarning("TEST MODE (manual): type an SLV and press <Enter> to send. Empty line to quit test mode.");
        }
        else
        {
            _rfid.TagReceived += OnTag;
            _log.LogInformation("Ready. Waiting for RFID tags…");
        }
        return Task.CompletedTask;
    }

    public Task StopAsync(CancellationToken cancellationToken)
    {
        if (_app.TestMode)
        {
            try { _manualCts?.Cancel(); } catch { }
        }
        else
        {
            _rfid.TagReceived -= OnTag;
        }
        return Task.CompletedTask;
    }

    private async Task ManualInputLoopAsync(CancellationToken ct)
    {
        while (!ct.IsCancellationRequested)
        {
            Console.Write("\nEnter SLV (empty to exit test): ");
            string? line;
            try
            {
                line = await Task.Run(Console.ReadLine, ct);
            }
            catch (OperationCanceledException) { break; }

            if (string.IsNullOrWhiteSpace(line))
            {
                _log.LogInformation("Leaving TEST MODE (manual).");
                break;
            }

            var slv = line.Trim();
            _log.LogInformation("Sending manual SLV='{slv}'", slv);
            await _signalR.PublishRfidAsync(slv);
        }
    }

    private async void OnTag(object? sender, TagEventArgs e)
    {
        try
        {
            var slv = await _repo.GetCarteSlvByRfidHexAsync(e.HexCanonical, CancellationToken.None);
            if (slv is null)
                Console.WriteLine($"[{e.Timestamp:HH:mm:ss}] HEX={e.HexCanonical} -> NOT FOUND");
            else
                Console.WriteLine($"[{e.Timestamp:HH:mm:ss}] HEX={e.HexCanonical} -> CarteSLV={slv}");
            await _signalR.PublishRfidAsync(slv);
        }
        catch (Exception ex)
        { _log.LogError(ex, "Lookup/send failed for HEX={hex}", e.HexCanonical); }
    }
}

#endregion

#region Program

public class Program
{
    public static async Task Main(string[] args)
    {
        var builder = WebApplication.CreateBuilder(args);

        // Configuration
        builder.Configuration
            .AddJsonFile("appsettings.json", optional: true, reloadOnChange: true)
            .AddJsonFile("appsettings.Development.json", optional: true)
            .AddEnvironmentVariables();

        // Configure services
        builder.Services.Configure<AppOptions>(builder.Configuration.GetSection("App"));
        builder.Services.Configure<RfidOptions>(builder.Configuration.GetSection("Rfid"));
        builder.Services.Configure<DbOptions>(opt =>
        {
            opt.ConnectionString = builder.Configuration.GetConnectionString("SqlServer")
                                 ?? throw new InvalidOperationException("ConnectionStrings:SqlServer missing");
            var table = builder.Configuration["Db:ClientEquipementsTable"];
            if (!string.IsNullOrWhiteSpace(table)) opt.ClientEquipementsTable = table!;
        });
        builder.Services.Configure<SignalROptions>(builder.Configuration.GetSection("SignalR"));

        // Singletons
        builder.Services.AddSingleton<RfidService>();
        builder.Services.AddSingleton<IClientEquipementRepository, ClientEquipementRepository>();
        builder.Services.AddSingleton<SignalRPublisher>();
        builder.Services.AddSingleton<RfidResolverService>();

        // Hosted services
        builder.Services.AddHostedService(sp => sp.GetRequiredService<SignalRPublisher>());

        var isTest = builder.Configuration.GetValue<bool>("App:TestMode");
        if (!isTest)
        {
            builder.Services.AddHostedService(sp => sp.GetRequiredService<RfidService>());
        }

        builder.Services.AddHostedService(sp => sp.GetRequiredService<RfidResolverService>());

        // Logging
        builder.Logging.ClearProviders();
        builder.Logging.AddSimpleConsole(o => { o.SingleLine = true; o.TimestampFormat = "HH:mm:ss "; });
        builder.Logging.SetMinimumLevel(LogLevel.Information);

        // Configure HTTP port
        var httpPort = builder.Configuration.GetValue<int>("App:HttpPort", 5000);
        builder.WebHost.UseUrls($"http://localhost:{httpPort}");

        var app = builder.Build();

        // HTTP Endpoint: GET /device-id
        app.MapGet("/device-id", () =>
        {
            var deviceId = DeviceIdManager.GetOrCreateDeviceId();
            var filePath = DeviceIdManager.GetDeviceIdFilePath();

            return Results.Json(new
            {
                deviceId,
                filePath,
                timestamp = DateTime.UtcNow
            });
        });

        // Root endpoint
        app.MapGet("/", () => Results.Json(new
        {
            message = "RFID Service API",
            endpoints = new[]
            {
                "/device-id - Get device identifier"
            }
        }));

        Console.WriteLine("RFID → SQL (CarteSLV) → Azure SignalR (slv_hub)");
        Console.WriteLine($"HTTP API listening on http://localhost:{httpPort}");
        Console.WriteLine($"Device ID file: {DeviceIdManager.GetDeviceIdFilePath()}");
        Console.WriteLine("Ctrl+C to exit.");

        await app.RunAsync();
    }
}

#endregion