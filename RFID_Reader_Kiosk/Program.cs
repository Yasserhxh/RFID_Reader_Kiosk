// Program.cs
// .NET 8 Console App — RFID -> SQL Server (Dapper) -> Azure SignalR (slv_hub)
// Test mode: when App:TestMode = true, publish CarteSLV="4421" on a timer (no RFID/DB)
//
// Packages:
//   dotnet add package Microsoft.Azure.SignalR.Management
//   dotnet add package Microsoft.Data.SqlClient
//   dotnet add package Dapper
//
// Run: dotnet run

using Dapper;
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
using System.Net.Sockets;
using System.Text;
using System.Text.RegularExpressions;
using System.Threading;
using System.Threading.Tasks;
using System.Security.Cryptography;
using System.Net;

#region Options

public sealed class AppOptions
{
    public bool TestMode { get; set; } = false;
    public int TestIntervalMs { get; set; } = 60000; // publish every second in test mode
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

    //public string HubName { get; set; } = "slv_pabentry_hub";
    //public string MethodName { get; set; } = "ReceivePabEntryRfid";

    //public string HubName { get; set; } = "slv_loading_hub";
    //public string MethodName { get; set; } = "ReceiveLoadingRfid";

    //public string HubName { get; set; } = "slv_pabexit_hub";
    //public string MethodName { get; set; } = "ReceivePabExitRfid";
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

#region SignalR publisher (single endpoint)

// Add to Program.cs (top of file)
// --- Device identity (id + secret) persisted to files ---
public static class DeviceIdentity
{
    private const string DeviceIdFile = "device.id";
    private const string DeviceSecretFile = "device.secret";

    public static (string deviceId, byte[] secret) GetOrCreate()
    {
        string deviceId;
        byte[] secret;

        if (File.Exists(DeviceIdFile) && File.Exists(DeviceSecretFile))
        {
            deviceId = File.ReadAllText(DeviceIdFile).Trim();
            var secretHex = File.ReadAllText(DeviceSecretFile).Trim();
            secret = Convert.FromHexString(secretHex);
            if (!string.IsNullOrWhiteSpace(deviceId) && secret.Length == 32)
                return (deviceId, secret);
        }

        deviceId = Guid.NewGuid().ToString("N");
        secret = RandomNumberGenerator.GetBytes(32); // 256-bit

        File.WriteAllText(DeviceIdFile, deviceId);
        File.WriteAllText(DeviceSecretFile, Convert.ToHexString(secret));
        return (deviceId, secret);
    }
}

public sealed class SignalRPublisher : IHostedService, IAsyncDisposable
{
    private readonly ILogger<SignalRPublisher> _log;
    private readonly SignalROptions _opt;
    private ServiceManager? _mgr;
    private ServiceHubContext? _hub;

    private readonly string _deviceId;
    private readonly byte[] _deviceSecret;
    private string GroupName => $"device:{_deviceId}";

    // loopback proof server
    private HttpListener? _listener;

    // === Registry API (same shape you used) ===
    private static readonly HttpClient _http = new() { Timeout = TimeSpan.FromSeconds(5) };
    private const string ApiBase = "http://172-189-107-115.nip.io/signalrapi";
    private const string RegistryEndpoint = ApiBase + "/api/device/register";
    private const string RegistryAdminKey = "dev-admin-key"; // align with your API

    public SignalRPublisher(ILogger<SignalRPublisher> log, IOptions<SignalROptions> opt)
    {
        _log = log;
        _opt = opt.Value;

        (_deviceId, _deviceSecret) = DeviceIdentity.GetOrCreate();
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

        // --- Register device with your backend so it can validate proofs ---
        await RegisterDeviceAsync();

        // --- Bind userId=deviceId to the per-device group (no CreateUserGroupManager needed) ---
        try
        {
            await _hub.UserGroups.AddToGroupAsync(_deviceId, GroupName, cancellationToken);
            _log.LogInformation("Bound userId={user} to group={group}", _deviceId, GroupName);
        }
        catch (Exception ex)
        {
            _log.LogWarning(ex, "AddToGroupAsync failed (ok until the first client connects).");
        }

        // --- Start loopback endpoint: GET http://127.0.0.1:5858/device-proof ---
        StartLoopbackProofServer();
    }

    public Task StopAsync(CancellationToken cancellationToken)
    {
        StopLoopbackProofServer();
        return Task.CompletedTask;
    }

    public async ValueTask DisposeAsync()
    {
        if (_hub is not null) await _hub.DisposeAsync();
    }

    // === Called by your resolver; now sends to per-device group on ReceiveRfid ===
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
            await _hub.Clients.Group(GroupName).SendAsync(_opt.MethodName, payload);
            _log.LogInformation("📡 Sent via SignalR to {group}: {payload}", GroupName, payload);
        }
        catch (Exception ex)
        {
            _log.LogWarning(ex, "SignalR send failed");
        }
    }

    // === Your requested method: register device (exact logic you showed) ===
    private async Task RegisterDeviceAsync()
    {
        try
        {
            var payload = System.Text.Json.JsonSerializer.Serialize(new
            {
                DeviceId = _deviceId,
                SecretHex = Convert.ToHexString(_deviceSecret)
            });
            using var req = new HttpRequestMessage(HttpMethod.Post, RegistryEndpoint);
            req.Headers.Add("X-Api-Key", RegistryAdminKey);
            req.Content = new StringContent(payload, Encoding.UTF8, "application/json");
            var resp = await _http.SendAsync(req);
            resp.EnsureSuccessStatusCode();
            _log.LogInformation("Device registered with API");
        }
        catch (Exception ex)
        {
            _log.LogWarning(ex, "Device registration failed; API won't be able to validate proof.");
        }
    }

    // === Loopback proof server (device-proof) ===
    private void StartLoopbackProofServer()
    {
        _listener = new HttpListener();
        _listener.Prefixes.Add("http://127.0.0.1:5858/");
        _listener.Start();
        _ = Task.Run(async () =>
        {
            _log.LogInformation("Loopback proof server listening at http://127.0.0.1:5858/");
            while (_listener.IsListening)
            {
                HttpListenerContext? ctx = null;
                try
                {
                    ctx = await _listener.GetContextAsync();

                    var origin = ctx.Request.Headers["Origin"];
                    ctx.Response.AddHeader("Access-Control-Allow-Origin", string.IsNullOrEmpty(origin) ? "*" : origin);
                    ctx.Response.AddHeader("Vary", "Origin");
                    ctx.Response.AddHeader("Access-Control-Allow-Methods", "GET, OPTIONS");
                    ctx.Response.AddHeader("Access-Control-Allow-Headers", "Content-Type");

                    if (ctx.Request.HttpMethod == "OPTIONS")
                    {
                        ctx.Response.StatusCode = 200;
                        ctx.Response.Close();
                        continue;
                    }

                    if (ctx.Request.HttpMethod == "GET" && ctx.Request.Url?.AbsolutePath == "/device-proof")
                    {
                        var ts = DateTimeOffset.UtcNow.ToUnixTimeSeconds().ToString();
                        var nonce = Guid.NewGuid().ToString("N");
                        var msg = $"{_deviceId}.{ts}.{nonce}";
                        using var h = new HMACSHA256(_deviceSecret);
                        var sigBytes = h.ComputeHash(Encoding.UTF8.GetBytes(msg));
                        var sigHex = Convert.ToHexString(sigBytes);

                        var json = $"{{\"deviceId\":\"{_deviceId}\",\"ts\":\"{ts}\",\"nonce\":\"{nonce}\",\"sig\":\"{sigHex}\"}}";
                        var bytes = Encoding.UTF8.GetBytes(json);

                        ctx.Response.StatusCode = 200;
                        ctx.Response.ContentType = "application/json; charset=utf-8";
                        await ctx.Response.OutputStream.WriteAsync(bytes, 0, bytes.Length);
                        ctx.Response.Close();
                        continue;
                    }

                    ctx.Response.StatusCode = 404;
                    ctx.Response.Close();
                }
                catch (Exception ex)
                {
                    try { ctx?.Response.Close(); } catch { }
                    _log.LogDebug(ex, "Loopback proof server request failed.");
                }
            }
        });

        // If you get 403 once on Windows:
        //   netsh http add urlacl url=http://127.0.0.1:5858/ user=Everyone
    }

    private void StopLoopbackProofServer()
    {
        try { _listener?.Stop(); } catch { }
        try { _listener?.Close(); } catch { }
        _listener = null;
    }

    #endregion

    #region Resolver (RFID -> DB -> SignalR) + Test Mode

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

        // ---- Manual test loop (console) ----
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

        // ---- Production path (RFID device) ----
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
            using var host = Host.CreateDefaultBuilder(args)
                .ConfigureAppConfiguration(cfg =>
                {
                    cfg.AddJsonFile("appsettings.json", optional: true, reloadOnChange: true)
                       .AddJsonFile("appsettings.Development.json", optional: true)
                       .AddEnvironmentVariables();
                })
                .ConfigureServices((ctx, services) =>
                {
                    services.Configure<AppOptions>(ctx.Configuration.GetSection("App"));
                    services.Configure<RfidOptions>(ctx.Configuration.GetSection("Rfid"));
                    services.Configure<DbOptions>(opt =>
                    {
                        opt.ConnectionString = ctx.Configuration.GetConnectionString("SqlServer")
                                             ?? throw new InvalidOperationException("ConnectionStrings:SqlServer missing");
                        var table = ctx.Configuration["Db:ClientEquipementsTable"];
                        if (!string.IsNullOrWhiteSpace(table)) opt.ClientEquipementsTable = table!;
                    });
                    services.Configure<SignalROptions>(ctx.Configuration.GetSection("SignalR"));

                    // Always register singletons
                    services.AddSingleton<RfidService>();
                    services.AddSingleton<IClientEquipementRepository, ClientEquipementRepository>();
                    services.AddSingleton<SignalRPublisher>();
                    services.AddSingleton<RfidResolverService>();

                    // Hosted services:
                    services.AddHostedService(sp => sp.GetRequiredService<SignalRPublisher>()); // warm hub

                    var isTest = ctx.Configuration.GetValue<bool>("App:TestMode");
                    if (!isTest)
                    {
                        // Only run RFID reader when not in test mode
                        services.AddHostedService(sp => sp.GetRequiredService<RfidService>());
                    }

                    services.AddHostedService(sp => sp.GetRequiredService<RfidResolverService>());
                })
                .ConfigureLogging(logging =>
                {
                    logging.ClearProviders();
                    logging.AddSimpleConsole(o => { o.SingleLine = true; o.TimestampFormat = "HH:mm:ss "; });
                    logging.SetMinimumLevel(LogLevel.Information);
                })
                .Build();

            Console.WriteLine("RFID → SQL (CarteSLV) → Azure SignalR (slv_hub). Ctrl+C to exit.");
            await host.RunAsync();
        }
    }
}

#endregion
