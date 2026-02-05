// Program.cs — .NET 8 (Merged)
// RFID (TCP) -> SQL (Card + Step) -> caches current Step -> Scale (TCP) -> Azure SignalR (route by Step)
// + HTTP API: /device-id, /status, /send-slv
//
// Packages:
//   dotnet add package Microsoft.Azure.SignalR.Management
//   dotnet add package Microsoft.Data.SqlClient
//   dotnet add package Dapper
//
// Run:
//   dotnet run

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
using System.Globalization;
using System.IO;
using System.Linq;
using System.Net.Sockets;
using System.Text;
using System.Text.RegularExpressions;

#region Device ID Manager

public static class DeviceIdManager
{
    private static readonly string DeviceIdFile;
    private static string? _cachedDeviceId;

    static DeviceIdManager()
    {
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

#region Options

public sealed class AppOptions
{
    public bool TestMode { get; set; } = false;
    public int HttpPort { get; set; } = 5001;
    public string? ForceDeviceId { get; set; }
}

public sealed class RfidOptions
{
    public string Host { get; set; } = "10.116.136.22";
    public string? FallbackHost { get; set; } = null;
    public int Port { get; set; } = 4001;
    public int ReconnectDelayMs { get; set; } = 2000;
    public int ReadBufferBytes { get; set; } = 4096;
    public int DebounceSeconds { get; set; } = 5;
    public string? LineTerminatorRegex { get; set; }
}

public sealed class ScaleOptions
{
    public string Host { get; set; } = "10.8.197.26";
    public int Port { get; set; } = 4001;
    public int ReadTimeoutMs { get; set; } = 3000;
    public int ReconnectDelayMs { get; set; } = 1500;

    public decimal Divisor { get; set; } = 1m;
    public int MinDigits { get; set; } = 3;

    public bool TestMode { get; set; } = false;
    public int TestTickMs { get; set; } = 150;
    public int TestMaxKg { get; set; } = 16000;
}

public sealed class DbOptions
{
    public string ConnectionString { get; set; } = default!;

    // RFID hex -> card (CarteSLV)
    public string ClientEquipementsTable { get; set; } = "dbo.Ecare_ClientEquipements";

    // card -> step
    public string OrderLegendTable { get; set; } = "dbo.Ecare_OrderLegend";
}

public sealed class SignalROptions
{
    public string ConnectionString { get; set; } = default!;

    // step -> hub/method routing for weight
    public Dictionary<int, SignalRTarget> Targets { get; set; } = new();

    // optional: publish RFID card events somewhere
    public string? RfidHubName { get; set; }
    public string? RfidMethodName { get; set; }
}

public sealed class SignalRTarget
{
    public string HubName { get; set; } = default!;
    public string MethodName { get; set; } = default!;
}

#endregion

#region Shared State

public interface IStepState
{
    void Update(string rfidCard, int? step, DateTime atUtc);
    (string? card, int? step, DateTime? atUtc) Snapshot();
}

public sealed class StepState : IStepState
{
    private readonly object _lock = new();
    private string? _card;
    private int? _step;
    private DateTime? _atUtc;

    public void Update(string rfidCard, int? step, DateTime atUtc)
    {
        lock (_lock)
        {
            _card = rfidCard;
            _step = step;
            _atUtc = atUtc;
        }
    }

    public (string? card, int? step, DateTime? atUtc) Snapshot()
    {
        lock (_lock) return (_card, _step, _atUtc);
    }
}

public interface IWeightState
{
    void Update(decimal weightKg, bool isStable, DateTime atUtc);
    (decimal? weightKg, bool? isStable, DateTime? atUtc) Snapshot();
}

public sealed class WeightState : IWeightState
{
    private readonly object _lock = new();
    private decimal? _w;
    private bool? _stable;
    private DateTime? _atUtc;

    public void Update(decimal weightKg, bool isStable, DateTime atUtc)
    {
        lock (_lock)
        {
            _w = weightKg;
            _stable = isStable;
            _atUtc = atUtc;
        }
    }

    public (decimal? weightKg, bool? isStable, DateTime? atUtc) Snapshot()
    {
        lock (_lock) return (_w, _stable, _atUtc);
    }
}

#endregion

#region RFID Service

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
                var hostToTry = new[] { _opt.Host, _opt.FallbackHost }
                    .Where(h => !string.IsNullOrWhiteSpace(h))
                    .ToArray();

                Exception? lastEx = null;

                TcpClient? tcp = null;
                foreach (var host in hostToTry)
                {
                    try
                    {
                        tcp = new TcpClient();
                        var connectTask = tcp.ConnectAsync(host!, _opt.Port);
                        var winner = await Task.WhenAny(connectTask, Task.Delay(_opt.ReconnectDelayMs, ct));
                        if (winner != connectTask) throw new TimeoutException($"RFID connect timeout ({host}:{_opt.Port})");
                        await connectTask;

                        _log.LogInformation("RFID connected to {host}:{port}", host, _opt.Port);
                        break;
                    }
                    catch (Exception ex)
                    {
                        lastEx = ex;
                        try { tcp?.Dispose(); } catch { }
                        tcp = null;
                    }
                }

                if (tcp is null)
                    throw lastEx ?? new Exception("RFID connect failed to all hosts.");

                using (tcp)
                {
                    using var stream = tcp.GetStream();

                    var buffer = ArrayPool<byte>.Shared.Rent(_opt.ReadBufferBytes);
                    var sb = new StringBuilder();

                    _log.LogInformation("RFID stream opened.");

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

                                _log.LogInformation("[RFID] Raw='{raw}' HEX={hex}", clean, hexCanonical);
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

#region Repository (RFID->Card + Card->Step)

public interface IRepository
{
    Task<string?> GetRfidCardByRfidHexAsync(string hexCanonical, CancellationToken ct);
    Task<int?> GetStepByRfidCardAsync(string rfidCard, CancellationToken ct);
}

public sealed class Repository : IRepository
{
    private readonly ILogger<Repository> _log;
    private readonly DbOptions _opt;

    public Repository(ILogger<Repository> log, IOptions<DbOptions> opt)
    { _log = log; _opt = opt.Value; }

    // HEX -> CarteSLV (card)
    public async Task<string?> GetRfidCardByRfidHexAsync(string hexCanonical, CancellationToken ct)
    {
        const string sqlTemplate = """
            SELECT TOP(1) CarteSLV
            FROM {TABLE}
            WHERE RfidHex = @hex
        """;

        var sql = sqlTemplate.Replace("{TABLE}", _opt.ClientEquipementsTable);

        await using var conn = new SqlConnection(_opt.ConnectionString);
        await conn.OpenAsync(ct);

        var card = await conn.QueryFirstOrDefaultAsync<string?>(
            new CommandDefinition(sql, new { hex = hexCanonical }, cancellationToken: ct));

        if (card is null) _log.LogWarning("No match for RfidHex={hex}", hexCanonical);
        else _log.LogInformation("Match: RfidHex={hex} -> RfidCard={card}", hexCanonical, card);

        return card;
    }

    // Card -> Step
    public async Task<int?> GetStepByRfidCardAsync(string rfidCard, CancellationToken ct)
    {
        if (string.IsNullOrWhiteSpace(rfidCard))
            return null;

        const string sqlTemplate = """
            SELECT TOP(1) Step
            FROM {TABLE}
            WHERE RfidCard = @rfidCard
        """;

        var sql = sqlTemplate.Replace("{TABLE}", _opt.OrderLegendTable);

        await using var conn = new SqlConnection(_opt.ConnectionString);
        await conn.OpenAsync(ct);

        object paramValue = rfidCard;
        if (int.TryParse(rfidCard.Trim(), out var asInt))
            paramValue = asInt;

        var step = await conn.QueryFirstOrDefaultAsync<int?>(
            new CommandDefinition(sql, new { rfidCard = paramValue }, cancellationToken: ct));

        if (step is null)
            _log.LogWarning("No Step found for RfidCard={rfidCard}", rfidCard);
        else
            _log.LogInformation("Legend: RfidCard={rfidCard} -> Step={step}", rfidCard, step);

        return step;
    }
}

#endregion

#region Azure SignalR Router Publisher (weights routed by step)

public sealed class SignalRRouterPublisher : IHostedService, IAsyncDisposable
{
    private readonly ILogger<SignalRRouterPublisher> _log;
    private readonly SignalROptions _opt;
    private readonly IStepState _stepState;
    private readonly string _deviceId;
    private readonly ServiceManager _mgr;

    private readonly Dictionary<string, ServiceHubContext> _hubCache = new(StringComparer.OrdinalIgnoreCase);

    public SignalRRouterPublisher(
        ILogger<SignalRRouterPublisher> log,
        IOptions<SignalROptions> opt,
        IOptions<AppOptions> appOpt,
        IStepState stepState,
        ServiceManager mgr)
    {
        _log = log;
        _opt = opt.Value;
        _stepState = stepState;
        _mgr = mgr;

        _deviceId = !string.IsNullOrWhiteSpace(appOpt.Value.ForceDeviceId)
            ? appOpt.Value.ForceDeviceId!
            : DeviceIdManager.GetOrCreateDeviceId();

        _log.LogInformation("Device ID: {deviceId}", _deviceId);
    }

    public async Task StartAsync(CancellationToken cancellationToken)
    {
        if (string.IsNullOrWhiteSpace(_opt.ConnectionString))
            throw new InvalidOperationException("SignalR:ConnectionString missing");
        if (_opt.Targets is null || _opt.Targets.Count == 0)
            throw new InvalidOperationException("SignalR:Targets missing/empty");

        foreach (var t in _opt.Targets.Values)
            await GetHubAsync(t.HubName, cancellationToken);

        if (!string.IsNullOrWhiteSpace(_opt.RfidHubName))
            await GetHubAsync(_opt.RfidHubName!, cancellationToken);

        _log.LogInformation("SignalR router ready.");
    }

    public Task StopAsync(CancellationToken cancellationToken) => Task.CompletedTask;

    private async Task<ServiceHubContext> GetHubAsync(string hubName, CancellationToken ct)
    {
        if (_hubCache.TryGetValue(hubName, out var cached))
            return cached;

        var hub = await _mgr.CreateHubContextAsync(hubName, ct);
        _hubCache[hubName] = hub;
        _log.LogInformation("Hub context ready: {hub}", hubName);
        return hub;
    }

    public async Task PublishWeightAsync(decimal weightKg, bool isStable, CancellationToken ct)
    {
        var (_, step, _) = _stepState.Snapshot();

        if (step is null)
        {
            _log.LogWarning("No step yet -> skipping weight publish.");
            return;
        }

        if (!_opt.Targets.TryGetValue(step.Value, out var target))
        {
            _log.LogWarning("No SignalR target configured for step={step} -> skipping.", step);
            return;
        }

        var hub = await GetHubAsync(target.HubName, ct);

        var payload = new
        {
            weight = decimal.Round(weightKg, 1, MidpointRounding.AwayFromZero),
            isStable,
            step,
            deviceId = _deviceId,
            tsUtc = DateTime.UtcNow
        };

        await hub.Clients.User(_deviceId).SendAsync(target.MethodName, payload, ct);

        _log.LogInformation("Weight sent step={step} hub={hub} method={method} payload={payload}",
            step, target.HubName, target.MethodName, System.Text.Json.JsonSerializer.Serialize(payload));
    }

    public async Task PublishRfidCardAsync(string? card, int? step, CancellationToken ct)
    {
        if (string.IsNullOrWhiteSpace(_opt.RfidHubName) || string.IsNullOrWhiteSpace(_opt.RfidMethodName))
            return;

        var hub = await GetHubAsync(_opt.RfidHubName!, ct);

        var payload = new
        {
            rfidCard = card,
            step,
            deviceId = _deviceId,
            tsUtc = DateTime.UtcNow
        };

        await hub.Clients.All.SendAsync(_opt.RfidMethodName!, payload, ct);

        _log.LogInformation("RFID event sent hub={hub} method={method} payload={payload}",
            _opt.RfidHubName, _opt.RfidMethodName, System.Text.Json.JsonSerializer.Serialize(payload));
    }

    public async ValueTask DisposeAsync()
    {
        foreach (var hub in _hubCache.Values)
            await hub.DisposeAsync();
        _hubCache.Clear();
    }
}

#endregion

#region RFID Resolver Hosted Service

public sealed class RfidResolverService : IHostedService
{
    private readonly ILogger<RfidResolverService> _log;
    private readonly AppOptions _app;
    private readonly RfidService _rfid;
    private readonly IRepository _repo;
    private readonly IStepState _stepState;
    private readonly SignalRRouterPublisher _pub;

    public RfidResolverService(
        ILogger<RfidResolverService> log,
        IOptions<AppOptions> app,
        RfidService rfid,
        IRepository repo,
        IStepState stepState,
        SignalRRouterPublisher pub)
    {
        _log = log;
        _app = app.Value;
        _rfid = rfid;
        _repo = repo;
        _stepState = stepState;
        _pub = pub;
    }

    public Task StartAsync(CancellationToken cancellationToken)
    {
        if (_app.TestMode)
        {
            _log.LogWarning("App.TestMode=true. RFID resolver not started.");
            return Task.CompletedTask;
        }

        _rfid.TagReceived += OnTag;
        _log.LogInformation("RFID Resolver ready.");
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
            var card = await _repo.GetRfidCardByRfidHexAsync(e.HexCanonical, CancellationToken.None);
            if (card is null)
            {
                Console.WriteLine($"[{e.Timestamp:HH:mm:ss}] HEX={e.HexCanonical} -> NOT FOUND");
                await _pub.PublishRfidCardAsync(null, null, CancellationToken.None);
                return;
            }

            var step = await _repo.GetStepByRfidCardAsync(card, CancellationToken.None);
            _stepState.Update(card, step, DateTime.UtcNow);

            Console.WriteLine($"[{e.Timestamp:HH:mm:ss}] HEX={e.HexCanonical} -> Card={card} Step={step?.ToString() ?? "NULL"}");
            await _pub.PublishRfidCardAsync(card, step, CancellationToken.None);
        }
        catch (Exception ex)
        {
            _log.LogError(ex, "RFID resolve failed for HEX={hex}", e.HexCanonical);
        }
    }
}

#endregion

#region Weight Parser + Weight Bridge

internal static class WeightParser
{
    private static readonly Regex ValueWithUnit = new(
        @"(?<!\S)(?<num>[+-]?\d+(?:[.,]\d+)?)[ ]*(?<unit>kg|g|t|lb|oz)\b",
        RegexOptions.IgnoreCase | RegexOptions.Compiled);

    private static readonly Regex AnyNumber = new(
        @"[+-]?\d+(?:[.,]\d+)?",
        RegexOptions.Compiled);

    public static bool TryParseWeight(string line, int minDigits, out decimal value)
    {
        var unitMatches = ValueWithUnit.Matches(line);
        if (unitMatches.Count > 0)
        {
            var m = unitMatches[^1];
            var raw = m.Groups["num"].Value;
            if (TryParseDecimalFlexible(raw, out value))
                return true;
        }

        Match? best = null;
        foreach (Match m in AnyNumber.Matches(line))
        {
            int digits = CountDigits(m.Value);
            if (digits >= minDigits && (best is null || m.Value.Length > best.Value.Length))
                best = m;
        }

        if (best is not null && TryParseDecimalFlexible(best.Value, out value))
            return true;

        value = default;
        return false;
    }

    private static int CountDigits(string s)
    {
        int c = 0;
        foreach (var ch in s)
            if (char.IsDigit(ch)) c++;
        return c;
    }

    private static bool TryParseDecimalFlexible(string s, out decimal v)
    {
        if (decimal.TryParse(s, NumberStyles.Float, CultureInfo.InvariantCulture, out v)) return true;
        if (decimal.TryParse(s, NumberStyles.Float, CultureInfo.GetCultureInfo("fr-FR"), out v)) return true;
        return false;
    }
}

public sealed class WeightBridgeService : BackgroundService
{
    private readonly ILogger<WeightBridgeService> _log;
    private readonly ScaleOptions _opt;
    private readonly SignalRRouterPublisher _pub;
    private readonly IWeightState _weightState;

    public WeightBridgeService(
        ILogger<WeightBridgeService> log,
        IOptions<ScaleOptions> opt,
        SignalRRouterPublisher pub,
        IWeightState weightState)
    {
        _log = log;
        _opt = opt.Value;
        _pub = pub;
        _weightState = weightState;
    }

    protected override async Task ExecuteAsync(CancellationToken stoppingToken)
    {
        if (_opt.TestMode)
        {
            var rnd = new Random();
            _log.LogWarning("Scale TEST MODE enabled.");
            while (!stoppingToken.IsCancellationRequested)
            {
                var w = rnd.Next(0, _opt.TestMaxKg);
                var isStable = rnd.Next(0, 10) == 0; // sometimes stable
                await PublishAsync(w, isStable, stoppingToken);
                await Task.Delay(_opt.TestTickMs, stoppingToken);
            }
            return;
        }

        // REAL LIVE MODE — BYTE READER (scope-safe)
        byte[] buffer = new byte[2048];

        while (!stoppingToken.IsCancellationRequested)
        {
            try
            {
                using var tcp = new TcpClient();
                _log.LogInformation("Connecting to scale {Host}:{Port} ...", _opt.Host, _opt.Port);

                await tcp.ConnectAsync(_opt.Host, _opt.Port, stoppingToken);

                if (!tcp.Connected)
                {
                    _log.LogWarning("Scale connection failed.");
                    await Task.Delay(_opt.ReconnectDelayMs, stoppingToken);
                    continue;
                }

                tcp.ReceiveTimeout = _opt.ReadTimeoutMs;
                _log.LogInformation("Scale connected to {Host}:{Port}", _opt.Host, _opt.Port);

                using var stream = tcp.GetStream();

                // Flush any garbage already buffered
                while (stream.DataAvailable)
                {
                    await stream.ReadAsync(buffer.AsMemory(0, buffer.Length), stoppingToken);
                }

                var pending = new List<byte>(8192);

                decimal lastValue = 0m;
                int stableCounter = 0;
                bool haveLast = false;

                while (!stoppingToken.IsCancellationRequested)
                {
                    int bytesRead = await stream.ReadAsync(buffer.AsMemory(0, buffer.Length), stoppingToken);
                    if (bytesRead <= 0) throw new IOException("Scale closed connection.");

                    for (int i = 0; i < bytesRead; i++) pending.Add(buffer[i]);

                    while (true)
                    {
                        int stx = pending.IndexOf(0x02);
                        if (stx < 0)
                        {
                            pending.Clear();
                            break;
                        }

                        if (stx > 0) pending.RemoveRange(0, stx);

                        int cr = pending.IndexOf(0x0D, startIndex: 1);
                        if (cr < 0) break;

                        var payloadBytes = pending.GetRange(1, cr - 1).ToArray();
                        pending.RemoveRange(0, cr + 1);

                        if (pending.Count > 0 && pending[0] != 0x02)
                            pending.RemoveAt(0);

                        string line = Encoding.ASCII.GetString(payloadBytes).Trim();
                        if (line.Length == 0) continue;

                        if (!WeightParser.TryParseWeight(line, _opt.MinDigits, out var raw))
                            continue;

                        var current = raw / _opt.Divisor;

                        if (haveLast && Math.Abs(current - lastValue) < 0.01m)
                            stableCounter++;
                        else
                            stableCounter = 1;

                        bool isStable = stableCounter >= 15 && current >= 2000m;

                        Console.WriteLine("Current Weight: " + current);

                        if (current >= 2000m)
                            await PublishAsync(current, isStable, stoppingToken);

                        lastValue = current;
                        haveLast = true;
                    }

                    if (pending.Count > 100_000) pending.Clear();
                }
            }
            catch (OperationCanceledException) { break; }
            catch (Exception ex)
            {
                _log.LogWarning(ex, "Scale loop error; reconnecting in {ms}ms...", _opt.ReconnectDelayMs);
                try { await Task.Delay(_opt.ReconnectDelayMs, stoppingToken); } catch { }
            }
        }
    }

    private async Task PublishAsync(decimal weightKg, bool isStable, CancellationToken ct)
    {
        _weightState.Update(weightKg, isStable, DateTime.UtcNow);
        await _pub.PublishWeightAsync(weightKg, isStable, ct);
    }
}

static class ByteListExtensions
{
    public static int IndexOf(this List<byte> data, byte value, int startIndex = 0)
    {
        for (int i = startIndex; i < data.Count; i++)
            if (data[i] == value) return i;
        return -1;
    }
}

#endregion

#region HTTP DTOs

public sealed record SendSlvRequest(string rfidCard);

#endregion

#region Program

public class Program
{
    public static async Task Main(string[] args)
    {
        var builder = WebApplication.CreateBuilder(args);

        builder.Configuration
            .AddJsonFile("appsettings.json", optional: true, reloadOnChange: true)
            .AddEnvironmentVariables();

        // Options
        builder.Services.Configure<AppOptions>(builder.Configuration.GetSection("App"));
        builder.Services.Configure<RfidOptions>(builder.Configuration.GetSection("Rfid"));
        builder.Services.Configure<ScaleOptions>(builder.Configuration.GetSection("Scale"));

        builder.Services.Configure<DbOptions>(opt =>
        {
            opt.ConnectionString = builder.Configuration.GetConnectionString("SqlServer")
                ?? throw new InvalidOperationException("ConnectionStrings:SqlServer missing");

            var t1 = builder.Configuration["Db:ClientEquipementsTable"];
            if (!string.IsNullOrWhiteSpace(t1)) opt.ClientEquipementsTable = t1!;

            var t2 = builder.Configuration["Db:OrderLegendTable"];
            if (!string.IsNullOrWhiteSpace(t2)) opt.OrderLegendTable = t2!;
        });

        builder.Services.AddOptions<SignalROptions>()
            .Bind(builder.Configuration.GetSection("SignalR"))
            .Validate(o => !string.IsNullOrWhiteSpace(o.ConnectionString), "SignalR:ConnectionString missing")
            .Validate(o => o.Targets is not null && o.Targets.Count > 0, "SignalR:Targets missing")
            .ValidateOnStart();

        // Shared state
        builder.Services.AddSingleton<IStepState, StepState>();
        builder.Services.AddSingleton<IWeightState, WeightState>();

        // Azure SignalR ServiceManager (shared)
        builder.Services.AddSingleton(sp =>
        {
            var sro = sp.GetRequiredService<IOptions<SignalROptions>>().Value;
            return new ServiceManagerBuilder()
                .WithOptions(o => o.ConnectionString = sro.ConnectionString)
                .BuildServiceManager();
        });

        // Services
        builder.Services.AddSingleton<RfidService>();
        builder.Services.AddSingleton<IRepository, Repository>();
        builder.Services.AddSingleton<SignalRRouterPublisher>();
        builder.Services.AddSingleton<RfidResolverService>();

        // Hosted services
        builder.Services.AddHostedService(sp => sp.GetRequiredService<SignalRRouterPublisher>());
        builder.Services.AddHostedService(sp => sp.GetRequiredService<RfidService>());
        builder.Services.AddHostedService(sp => sp.GetRequiredService<RfidResolverService>());
        builder.Services.AddHostedService<WeightBridgeService>();

        // Logging
        builder.Logging.ClearProviders();
        builder.Logging.AddSimpleConsole(o =>
        {
            o.SingleLine = true;
            o.TimestampFormat = "HH:mm:ss ";
        });
        builder.Logging.SetMinimumLevel(LogLevel.Information);

        // HTTP port
        var httpPort = builder.Configuration.GetValue<int>("App:HttpPort", 5001);
        builder.WebHost.UseUrls($"http://localhost:{httpPort}");

        builder.Services.AddCors();

        var app = builder.Build();
        app.UseCors(p => p.AllowAnyOrigin().AllowAnyHeader().AllowAnyMethod());

        // GET /device-id
        app.MapGet("/device-id", () =>
        {
            var deviceId = DeviceIdManager.GetOrCreateDeviceId();
            var filePath = DeviceIdManager.GetDeviceIdFilePath();
            return Results.Json(new { deviceId, filePath, timestamp = DateTime.UtcNow });
        });

        // GET /status
        app.MapGet("/status", (IStepState step, IWeightState weight) =>
        {
            var s = step.Snapshot();
            var w = weight.Snapshot();
            return Results.Json(new
            {
                step = s.step,
                rfidCard = s.card,
                rfidAtUtc = s.atUtc,
                lastWeightKg = w.weightKg,
                lastWeightStable = w.isStable,
                lastWeightAtUtc = w.atUtc,
                nowUtc = DateTime.UtcNow
            });
        });

        // POST /send-slv  { "rfidCard": "4421" }
        app.MapPost("/send-slv", async (
            SendSlvRequest req,
            IRepository repo,
            IStepState stepState,
            SignalRRouterPublisher pub,
            ILoggerFactory lf,
            CancellationToken ct) =>
        {
            var log = lf.CreateLogger("SendSlv");

            if (req is null || string.IsNullOrWhiteSpace(req.rfidCard))
                return Results.BadRequest(new { error = "rfidCard is required" });

            var card = req.rfidCard.Trim();

            var step = await repo.GetStepByRfidCardAsync(card, ct);
            stepState.Update(card, step, DateTime.UtcNow);

            await pub.PublishRfidCardAsync(card, step, ct);

            log.LogInformation("Manual /send-slv rfidCard={card} step={step}", card, step);

            return Results.Ok(new
            {
                rfidCard = card,
                step,
                nowUtc = DateTime.UtcNow
            });
        });

        // Root
        app.MapGet("/", () => Results.Json(new
        {
            message = "Merged RFID + Weight → Azure SignalR (step-routed)",
            endpoints = new[] { "/device-id", "/status", "/send-slv" }
        }));

        Console.WriteLine("Merged RFID + Weight → Azure SignalR (step-routed)");
        Console.WriteLine($"HTTP API listening on http://localhost:{httpPort}");
        Console.WriteLine($"Device ID file: {DeviceIdManager.GetDeviceIdFilePath()}");
        Console.WriteLine("Ctrl+C to exit.");

        await app.RunAsync();
    }
}

#endregion
