// .NET 8 Console/Minimal API — RFID -> SQL Server (Dapper) -> Azure SignalR
// Single EXE, embedded PDB, embedded appsettings.json
// DeviceId is ALWAYS generated on first run (GUID) then persisted; no ForceDeviceId support.

using Azure;
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
using System.Buffers;
using System.Diagnostics;
using System.Net.Sockets;
using System.Reflection;
using System.Security.Principal;
using System.Text;
using System.Text.Json;
using System.Text.RegularExpressions;

#region Options

public sealed class AppOptions
{
    public bool TestMode { get; set; } = false;
    public int TestIntervalMs { get; set; } = 60000;
    public int HttpPort { get; set; } = 5003;
}

public sealed class RfidOptions
{
    public string Host { get; set; } = "10.8.197.14";
    public string? FallbackHost { get; set; } = "10.8.197.14";
    public int Port { get; set; } = 4001;
    public int ReconnectDelayMs { get; set; } = 2000;
    public int ReadBufferBytes { get; set; } = 4096;
    public int DebounceSeconds { get; set; } = 5;
    public string? LineTerminatorRegex { get; set; }
}

public sealed class DbOptions
{
    public string ConnectionString { get; set; } = default!;
    public string TagsTable { get; set; } = "dbo.Ecare_Tags";
}

public sealed class SignalROptions
{
    public string ConnectionString { get; set; } = default!;
    public string HubName { get; set; } = "slv_hub";
    public string MethodName { get; set; } = "ReceiveRfid";
}

public sealed class DeviceOptions
{
    public string? Name { get; set; }
    public string? Site { get; set; }
}

#endregion

#region DTO for POST
public sealed class SlvRequest
{
    public string? Slv { get; set; }
}
#endregion

#region Device Identity

public static class DeviceIdentity
{
    private static readonly string DeviceIdFile;
    private static readonly string DeviceNameFile;
    private static string? _cachedDeviceId;
    private static string? _cachedDeviceName;

    static DeviceIdentity()
    {
        var baseDirectory = AppContext.BaseDirectory;
        DeviceIdFile = Path.Combine(baseDirectory, "device.id.txt");
        DeviceNameFile = Path.Combine(baseDirectory, "device.name.txt");
    }

    public static string GetOrCreateDeviceId()
    {
        if (_cachedDeviceId is not null) return _cachedDeviceId;

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

    public static string GetOrCreateDeviceName(string? configuredName = null)
    {
        if (_cachedDeviceName is not null) return _cachedDeviceName;

        if (File.Exists(DeviceNameFile))
        {
            var existing = File.ReadAllText(DeviceNameFile).Trim();
            if (!string.IsNullOrWhiteSpace(existing))
            {
                _cachedDeviceName = existing;
                return existing;
            }
        }

        var deviceId = GetOrCreateDeviceId();
        var last4 = deviceId.Length >= 4 ? deviceId[^4..] : deviceId;
        var defaultName = $"kiosk--{last4}".ToLowerInvariant();

        var name = string.IsNullOrWhiteSpace(configuredName) ? defaultName : configuredName.Trim();
        File.WriteAllText(DeviceNameFile, name);
        _cachedDeviceName = name;
        return name;
    }

    public static string GetDeviceIdPath() => DeviceIdFile;
    public static string GetDeviceNamePath() => DeviceNameFile;
}

#endregion

#region Tag Event Args

public sealed class TagEventArgs : EventArgs
{
    public required string RawAscii { get; init; }
    public required string HexCanonical { get; init; }
    public required DateTime Timestamp { get; init; }
}

#endregion

#region RFID Service

public sealed class RfidService : IHostedService
{
    private readonly ILogger<RfidService> _log;
    private readonly RfidOptions _opt;
    private CancellationTokenSource? _cts;

    public event EventHandler<TagEventArgs>? TagReceived;

    public RfidService(ILogger<RfidService> log, IOptions<RfidOptions> opt)
    {
        _log = log;
        _opt = opt.Value;
    }

    public Task StartAsync(CancellationToken cancellationToken)
    {
        _cts = CancellationTokenSource.CreateLinkedTokenSource(cancellationToken);
        _ = Task.Run(() => RunAsync(_cts.Token));
        return Task.CompletedTask;
    }

    public Task StopAsync(CancellationToken cancellationToken)
    {
        try { _cts?.Cancel(); } catch { }
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
                string[] hosts = new[]
                {
                    _opt.Host,
                    _opt.FallbackHost ?? ""
                };

                TcpClient? tcp = null;

                foreach (var host in hosts)
                {
                    if (string.IsNullOrWhiteSpace(host)) continue;

                    try
                    {
                        tcp = new TcpClient();

                        var connectTask = tcp.ConnectAsync(host, _opt.Port);
                        var winner = await Task.WhenAny(connectTask, Task.Delay(_opt.ReconnectDelayMs, ct));

                        if (winner == connectTask)
                        {
                            await connectTask;
                            _log.LogInformation("RFID connected to {host}:{port}", host, _opt.Port);
                            break;
                        }

                        _log.LogWarning("Timeout connecting to {host}:{port}", host, _opt.Port);
                    }
                    catch (Exception ex)
                    {
                        _log.LogWarning(ex, "Failed to connect to {host}:{port}", host, _opt.Port);
                    }
                }

                if (tcp == null || !tcp.Connected)
                {
                    _log.LogError("Both primary and fallback RFID endpoints failed. Retrying in {ms}ms...", _opt.ReconnectDelayMs);
                    await Task.Delay(_opt.ReconnectDelayMs, ct);
                    continue;
                }

                using var stream = tcp.GetStream();
                var buffer = ArrayPool<byte>.Shared.Rent(_opt.ReadBufferBytes);
                var sb = new StringBuilder();

                _log.LogInformation("RFID connected to {host}:{port}", _opt.Host, _opt.Port);

                try
                {
                    while (!ct.IsCancellationRequested)
                    {
                        if (!stream.DataAvailable)
                        {
                            await Task.Delay(5, ct);
                            continue;
                        }

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
                finally
                {
                    ArrayPool<byte>.Shared.Return(buffer);
                }
            }
            catch (OperationCanceledException)
            {
                break;
            }
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
            for (int i = 0; i < lines.Length - 1; i++)
                yield return lines[i];

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
        foreach (var ch in s)
            if (!char.IsControl(ch))
                b.Append(ch);

        return b.ToString();
    }

    private static string ToCanonicalHex(string ascii)
    {
        var hexLike = Regex.IsMatch(ascii, "^[0-9A-Fa-f]+$");
        if (hexLike && ascii.Length % 2 == 0)
            return ascii.ToUpperInvariant();

        var bytes = Encoding.ASCII.GetBytes(ascii);
        var sb = new StringBuilder(bytes.Length * 2);

        foreach (var b in bytes)
            sb.Append(b.ToString("X2"));

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
        _log = log;
        _opt = opt.Value;
    }

    public async Task<string?> GetCarteSlvByRfidHexAsync(string hexCanonical, CancellationToken ct)
    {
        const string sqlTemplate = """
            SELECT TOP(1) CarteSLV
            FROM {TABLE}
            WHERE RfidHex = @hex
        """;

        var sql = sqlTemplate.Replace("{TABLE}", _opt.TagsTable);
        await using var conn = new SqlConnection(_opt.ConnectionString);
        await conn.OpenAsync(ct);

        var slv = await conn.QueryFirstOrDefaultAsync<string?>(
            new CommandDefinition(sql, new { hex = hexCanonical }, cancellationToken: ct));

        if (slv is null)
            _log.LogWarning("No match for RfidHex={hex}", hexCanonical);
        else
            _log.LogInformation("Match: RfidHex={hex} -> CarteSLV={slv}", hexCanonical, slv);

        return slv;
    }
}

#endregion

#region SignalR Publisher

public sealed class SignalRPublisher : IHostedService, IAsyncDisposable
{
    private readonly ILogger<SignalRPublisher> _log;
    private readonly SignalROptions _opt;
    private readonly DeviceOptions _dev;
    private ServiceManager? _mgr;
    private ServiceHubContext? _hub;
    private readonly string _deviceId;
    private readonly string _deviceName;

    public SignalRPublisher(
        ILogger<SignalRPublisher> log,
        IOptions<SignalROptions> opt,
        IOptions<DeviceOptions> dev)
    {
        _log = log;
        _opt = opt.Value;
        _dev = dev.Value;

        _deviceId = DeviceIdentity.GetOrCreateDeviceId();
        _deviceName = DeviceIdentity.GetOrCreateDeviceName(_dev.Name);

        _log.LogInformation("Device ID: {deviceId} | Device Name: {deviceName}",
            _deviceId, _deviceName);
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

    public Task StopAsync(CancellationToken cancellationToken)
        => Task.CompletedTask;

    public async ValueTask DisposeAsync()
    {
        if (_hub is not null)
            await _hub.DisposeAsync();

        _mgr?.Dispose();
    }

    public async Task PublishRfidAsync(string? carteSlv)
    {
        if (_hub is null) return;

        var payload = new
        {
            carteSlv,
            deviceId = _deviceId,
            deviceName = _deviceName,
            tsUtc = DateTime.UtcNow
        };

        try
        {
            await _hub.Clients.All.SendAsync(_opt.MethodName, payload);
            _log.LogInformation("📡 Sent via SignalR: {payload}", JsonSerializer.Serialize(payload));
        }
        catch (Exception ex)
        {
            _log.LogWarning(ex, "SignalR send failed");
        }
    }
}

#endregion

#region Device Registration

public sealed class DeviceRegistrationService : IHostedService
{
    private readonly ILogger<DeviceRegistrationService> _log;
    private readonly DbOptions _db;
    private readonly DeviceOptions _dev;

    public DeviceRegistrationService(
        ILogger<DeviceRegistrationService> log,
        IOptions<DbOptions> db,
        IOptions<DeviceOptions> dev)
    {
        _log = log;
        _db = db.Value;
        _dev = dev.Value;
    }

    public async Task StartAsync(CancellationToken cancellationToken)
    {
        var deviceId = DeviceIdentity.GetOrCreateDeviceId();
        var deviceName = DeviceIdentity.GetOrCreateDeviceName(_dev.Name);
        var host = Environment.MachineName;
        var appVersion = typeof(Program).Assembly.GetName().Version?.ToString() ?? "1.0.0";

        const string upsertDevice = """
        MERGE dbo.Ecare_Device AS t
        USING (SELECT @DeviceId AS DeviceId) AS s
        ON (t.DeviceId = s.DeviceId)
        WHEN MATCHED THEN UPDATE SET
            Alias = @DeviceName,
            HostName = COALESCE(@HostName, t.HostName),
            AppVersion = @AppVersion,
            UpdatedAtUtc = SYSUTCDATETIME()
        WHEN NOT MATCHED THEN INSERT (DeviceId, Alias, HostName, AppVersion)
        VALUES (@DeviceId, @DeviceName, @HostName, @AppVersion);
        """;

        try
        {
            await using var conn = new SqlConnection(_db.ConnectionString);
            await conn.OpenAsync(cancellationToken);

            await conn.ExecuteAsync(new CommandDefinition(
                upsertDevice,
                new
                {
                    DeviceId = deviceId,
                    DeviceName = deviceName,
                    HostName = host,
                    AppVersion = appVersion
                },
                cancellationToken: cancellationToken
            ));

            _log.LogInformation(
                "Device registered: {deviceId} as '{deviceName}' (host {host})",
                deviceId, deviceName, host);
        }
        catch (Exception ex)
        {
            _log.LogError(ex, "Failed to register device info in dbo.Ecare_Device");
        }
    }

    public Task StopAsync(CancellationToken cancellationToken)
        => Task.CompletedTask;
}

#endregion

#region Resolver Service

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
        _log = log;
        _rfid = rfid;
        _repo = repo;
        _signalR = signalR;
        _app = app.Value;
    }

    public Task StartAsync(CancellationToken cancellationToken)
    {
        if (_app.TestMode)
        {
            _manualCts = CancellationTokenSource.CreateLinkedTokenSource(cancellationToken);
            _ = Task.Run(() => ManualInputLoopAsync(_manualCts.Token));
            _log.LogWarning("TEST MODE (manual): type an SLV and press <Enter> to send.");
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
                line = await Task.Run(Console.ReadLine!, ct);
            }
            catch (OperationCanceledException)
            {
                break;
            }

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
        {
            _log.LogError(ex, "Lookup/send failed for HEX={hex}", e.HexCanonical);
        }
    }
}

#endregion

#region Config Loader

static class ConfigLoader
{
    public static IConfiguration BuildWithEmbeddedFallback()
    {
        var baseDir = AppContext.BaseDirectory;
        var external = Path.Combine(baseDir, "appsettings.json");

        var cb = new ConfigurationBuilder()
            .SetBasePath(baseDir)
            .AddEnvironmentVariables();

        if (File.Exists(external))
        {
            cb.AddJsonFile("appsettings.json", optional: false, reloadOnChange: true);
            cb.AddJsonFile("appsettings.Development.json", optional: true);
            Console.WriteLine("Config source: external appsettings.json");
        }
        else
        {
            var asm = Assembly.GetExecutingAssembly();
            var name = asm.GetManifestResourceNames()
                          .FirstOrDefault(n => n.EndsWith("appsettings.json", StringComparison.OrdinalIgnoreCase));

            if (name is not null)
            {
                using var stream = asm.GetManifestResourceStream(name)!;
                cb.AddJsonStream(stream);
                Console.WriteLine("Config source: embedded appsettings.json");
            }
            else
            {
                Console.WriteLine("Config source: environment only (no appsettings.json found).");
            }
        }

        return cb.Build();
    }
}

#endregion

#region Program

public class Program
{
    public static async Task Main(string[] args)
    {
        var configuration = ConfigLoader.BuildWithEmbeddedFallback();

        var builder = WebApplication.CreateBuilder(args);
        builder.Configuration.AddConfiguration(configuration);

        // Options Binding
        builder.Services.Configure<AppOptions>(builder.Configuration.GetSection("App"));
        builder.Services.Configure<RfidOptions>(builder.Configuration.GetSection("Rfid"));
        builder.Services.Configure<DeviceOptions>(builder.Configuration.GetSection("Device"));
        builder.Services.Configure<DbOptions>(opt =>
        {
            opt.ConnectionString =
                builder.Configuration.GetConnectionString("SqlServer")
                ?? throw new InvalidOperationException("ConnectionStrings:SqlServer missing");

            var table = builder.Configuration["Db:TagsTable"];
            if (!string.IsNullOrWhiteSpace(table))
                opt.TagsTable = table!;
        });
        builder.Services.Configure<SignalROptions>(builder.Configuration.GetSection("SignalR"));

        // Core Services
        builder.Services.AddSingleton<RfidService>();
        builder.Services.AddSingleton<IClientEquipementRepository, ClientEquipementRepository>();
        builder.Services.AddSingleton<SignalRPublisher>();
        builder.Services.AddSingleton<RfidResolverService>();

        // Hosted Services (ordering preserved)
        builder.Services.AddHostedService(sp => sp.GetRequiredService<SignalRPublisher>());
        builder.Services.AddHostedService<DeviceRegistrationService>();

        var appOptions = configuration.GetSection("App").Get<AppOptions>() ?? new AppOptions();

        if (!appOptions.TestMode)
            builder.Services.AddHostedService(sp => sp.GetRequiredService<RfidService>());

        builder.Services.AddHostedService(sp => sp.GetRequiredService<RfidResolverService>());

        // Logging
        builder.Logging.ClearProviders();
        builder.Logging.AddSimpleConsole(o =>
        {
            o.SingleLine = true;
            o.TimestampFormat = "HH:mm:ss ";
        });
        builder.Logging.SetMinimumLevel(LogLevel.Information);

        // HTTP Server + CORS
        var httpPort = configuration.GetValue<int>("App:HttpPort", 5002);
        builder.WebHost.UseUrls($"http://0.0.0.0:{httpPort}");
        builder.Services.AddCors();

        var app = builder.Build();

        app.Use(async (ctx, next) =>
        {
            var origin = ctx.Request.Headers["Origin"];

            ctx.Response.Headers.Add("Access-Control-Allow-Origin",
                string.IsNullOrEmpty(origin) ? "*" : origin);

            ctx.Response.Headers.Add("Vary", "Origin");
            ctx.Response.Headers.Add("Access-Control-Allow-Methods", "GET, POST, OPTIONS");
            ctx.Response.Headers.Add("Access-Control-Allow-Headers", "Content-Type");

            if (ctx.Request.Method.Equals("OPTIONS", StringComparison.OrdinalIgnoreCase))
            {
                ctx.Response.StatusCode = StatusCodes.Status204NoContent;
                return;
            }

            await next();
        });

        // GET: /device-id
        app.MapGet("/device-id", () =>
        {
            var deviceId = DeviceIdentity.GetOrCreateDeviceId();

            return Results.Json(new
            {
                deviceId,
                filePath = DeviceIdentity.GetDeviceIdPath(),
                timestamp = DateTime.UtcNow
            });
        });

        // GET: /device-info
        app.MapGet("/device-info", (IOptions<DeviceOptions> devOpt) =>
        {
            var deviceId = DeviceIdentity.GetOrCreateDeviceId();
            var deviceName = DeviceIdentity.GetOrCreateDeviceName(devOpt.Value.Name);

            return Results.Json(new
            {
                deviceId,
                deviceName,
                idFile = DeviceIdentity.GetDeviceIdPath(),
                nameFile = DeviceIdentity.GetDeviceNamePath(),
                hostName = Environment.MachineName,
                timestamp = DateTime.UtcNow
            });
        });

        // POST: /send-slv (YOUR NEW ENDPOINT)
        app.MapPost("/send-slv", async (
            SlvRequest req,
            IOptions<AppOptions> appOpt,
            SignalRPublisher publisher) =>
        {
            if (req is null || string.IsNullOrWhiteSpace(req.Slv))
                return Results.BadRequest(new { error = "Missing SLV" });

            var slv = req.Slv.Trim();

            // TEST MODE → use posted SLV instead of manual input
            if (appOpt.Value.TestMode)
            {
                await publisher.PublishRfidAsync(slv);

                return Results.Json(new
                {
                    mode = "TEST",
                    sent = true,
                    slv,
                    timestamp = DateTime.UtcNow
                });
            }

            // REAL MODE → publish directly, no RFID lookup
            await publisher.PublishRfidAsync(slv);

            return Results.Json(new
            {
                mode = "REAL",
                sent = true,
                slv,
                timestamp = DateTime.UtcNow
            });
        });

        // Root
        app.MapGet("/", () =>
        {
            return Results.Json(new
            {
                message = "RFID Service API",
                endpoints = new[]
                {
                    "/device-id",
                    "/device-info",
                    "/send-slv"
                }
            });
        });

        Console.WriteLine("RFID → SQL (CarteSLV) → Azure SignalR");
            Console.WriteLine($"HTTP API: http://localhost:{httpPort}");
            Console.WriteLine($"Device ID file:   {DeviceIdentity.GetDeviceIdPath()}");
            Console.WriteLine($"Device Name file: {DeviceIdentity.GetDeviceNamePath()}");
            Console.WriteLine("Ctrl+C to exit.");

            // Get local IPv4 address (LAN IP)
            string localIp = "localhost";
            try
            {
                localIp = System.Net.Dns.GetHostEntry(System.Net.Dns.GetHostName())
                    .AddressList
                    .FirstOrDefault(a => a.AddressFamily == System.Net.Sockets.AddressFamily.InterNetwork)?
                    .ToString() ?? "localhost";
            }
            catch { }

            // PRINT FULL URLs
            Console.WriteLine("==========================================");
            Console.WriteLine($"Local IP detected: {localIp}");
            Console.WriteLine($"HTTP API     : http://{localIp}:{httpPort}");
            Console.WriteLine($"POST SLV     : http://{localIp}:{httpPort}/send-slv");
            Console.WriteLine("==========================================");

        try
        {
            WindowsFirewall.EnsureAllowInboundTcpForCurrentExeOnPort(
                httpPort,
                ruleName: $"RFID Service API (TCP {httpPort})",
                includeDomainProfile: true,
                includePrivateProfile: true);
            Console.WriteLine("Windows Firewall rule added.");
            Console.WriteLine("If you see a Windows Firewall prompt, please allow it to enable API access.");
        }
        catch (Exception ex)
        {
            // If not admin, Windows will keep prompting. Log and continue.
            app.Logger.LogWarning(ex, "Could not create Windows Firewall rule (admin required).");
        }
        await app.RunAsync();
        }
    }

#endregion

#region Windows Firewall Helper (Windows only)

    public static class WindowsFirewall
    {
        // Profiles bitmask used by INetFwRule.Profiles
        private const int PROFILE_DOMAIN = 1;
        private const int PROFILE_PRIVATE = 2;
        private const int PROFILE_PUBLIC = 4;

        public static void EnsureAllowInboundTcpForCurrentExeOnPort(
            int port,
            string ruleName,
            bool includeDomainProfile = true,
            bool includePrivateProfile = true)
        {
            if (!OperatingSystem.IsWindows())
                return;

            if (!IsAdministrator())
                throw new InvalidOperationException("Firewall rule creation requires Administrator privileges.");

            var exePath = Environment.ProcessPath
                ?? Process.GetCurrentProcess().MainModule?.FileName
                ?? throw new InvalidOperationException("Cannot resolve current process path.");

            dynamic policy2 = Activator.CreateInstance(Type.GetTypeFromProgID("HNetCfg.FwPolicy2")!)
                ?? throw new InvalidOperationException("Cannot create HNetCfg.FwPolicy2 (firewall COM).");

            // Avoid duplicates (same rule name)
            foreach (dynamic r in policy2.Rules)
            {
                string name = r.Name;
                if (string.Equals(name, ruleName, StringComparison.OrdinalIgnoreCase))
                    return; // already exists
            }

            dynamic rule = Activator.CreateInstance(Type.GetTypeFromProgID("HNetCfg.FWRule")!)
                ?? throw new InvalidOperationException("Cannot create HNetCfg.FWRule.");

            rule.Name = ruleName;
            rule.Description = "Auto-added by app to avoid Windows Firewall prompt.";
            rule.ApplicationName = exePath;

            rule.Protocol = 6;                 // TCP
            rule.LocalPorts = port.ToString(); // limit to your HTTP port
            rule.Direction = 1;                // Inbound
            rule.Action = 1;                   // Allow
            rule.Enabled = true;
            rule.InterfaceTypes = "All";

            int profiles = 0;
            if (includeDomainProfile) profiles |= PROFILE_DOMAIN;
            if (includePrivateProfile) profiles |= PROFILE_PRIVATE;
            // Do NOT include public unless you really want it:
            // profiles |= PROFILE_PUBLIC;

            rule.Profiles = profiles;

            policy2.Rules.Add(rule);
        }

        private static bool IsAdministrator()
        {
            using var identity = WindowsIdentity.GetCurrent();
            var principal = new WindowsPrincipal(identity);
            return principal.IsInRole(WindowsBuiltInRole.Administrator);
        }
    }

#endregion
