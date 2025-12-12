using System.Diagnostics;
using System.Globalization;
using System.IO;
using System.Linq;
using System.Net;
using System.Net.Http;
using System.Net.Http.Headers;
using System.Runtime.InteropServices;
using System.Security.Cryptography;
using System.Text;
using System.Text.Encodings.Web;
using System.Text.Json;
using System.Text.RegularExpressions;
using System.Text.Unicode;
using System.Threading.Channels;
using Microsoft.Playwright;
using Serilog;

namespace CopyReader
{
    internal class SocketMonitor
    {
        private IPlaywright _playwright;
        private IBrowser _browser;
        private IPage _page;
        private IBrowserContext _context;

        private readonly Dictionary<(long Account, string Symbol), EarlyExecAgg> _earlyExec = new();
        private readonly Dictionary<(long Account, string Symbol), DateTime> _lastExecEmitAt = new();
        private readonly Dictionary<(long Account, string Symbol), PendingOpen> _pendingOpens = new();
        private readonly Dictionary<(long Account, string Symbol), PendingReverse> _pendingReverses = new();
        private readonly Dictionary<(long, string), PositionCycle> _posicoesAtivas = new();
        private readonly Dictionary<OrderKey, OrderState> _ultimoEstadoPorChave = new();

        private readonly HashSet<string> _hashesProcessados = new();

        private const int AdjustDebounceMs = 750;
        private const int EarlyExecGraceMs = 450;
        private const int FlushPeriodMs = 100;

        private sealed record AdjustLog(string Symbol, string Dir, decimal QtyAbs, string Infos);

        private readonly object _sync = new();

        //private static readonly HttpClient _http = new HttpClient { Timeout = TimeSpan.FromSeconds(7) };
        private static readonly SocketsHttpHandler _handlerSocket = new()
        {
            PooledConnectionLifetime = TimeSpan.FromMinutes(5),
            PooledConnectionIdleTimeout = TimeSpan.FromMinutes(2),
            MaxConnectionsPerServer = 8,
            ConnectTimeout = TimeSpan.FromSeconds(2),
            AutomaticDecompression = DecompressionMethods.GZip | DecompressionMethods.Deflate
        };
        private static readonly HttpClient _http = new(_handlerSocket)
        {
            Timeout = TimeSpan.FromSeconds(5)
        };


        private const string ForwardUrl = "https://copycapi-48479afb2dfb.herokuapp.com/forward";
        private static bool Diff(decimal? a, decimal? b) => a.HasValue != b.HasValue || (a.HasValue && b.HasValue && a.Value != b.Value);
        private static bool IsBreakEven(decimal openPrice, decimal? sl) => sl.HasValue && sl.Value == openPrice;

        private readonly struct OrderKey
        {
            public readonly long Id;
            public readonly string Type;
            public OrderKey(long id, string type) { Id = id; Type = type ?? ""; }
            public override string ToString() => $"{Id}|{Type}";
        }

        private sealed class EarlyExecAgg
        {
            public decimal QtyAbs;
            public decimal LastPrice;
            public DateTime LastAtUtc;
        }

        private sealed class PendingOpen
        {
            public int Action;
            public string Symbol = "";
            public decimal Avg;
            public decimal Position;
            public decimal? Tp;
            public decimal? Sl;
            public DateTime CreatedAtUtc;
        }

        private sealed class PendingReverse
        {
            public int Action;
            public string Symbol = "";
            public decimal Avg;
            public decimal Position;
            public decimal? Tp;
            public decimal? Sl;
            public DateTime CreatedAtUtc;
        }

        private sealed class OrderState
        {
            public decimal Price { get; set; }
            public string Status { get; set; } = "";
            public bool HasTp { get; set; }
            public decimal Tp { get; set; }
            public bool HasSl { get; set; }
            public decimal Sl { get; set; }
            public string LastJson { get; set; } = "";
        }

        private sealed class PositionCycle
        {
            public DateTime OpenTs;
            public string Dir = "";
            public decimal Qty;
            public decimal OpenPrice, LastAvg;
            public decimal? SlStart, SlEnd;
            public decimal? TpStart, TpEnd;
            public bool SlChanged, TpChanged, TpCancelled, TpSetLater;

            public List<string> PendingInfos = new();
            public DateTime? PendingSince;

            public decimal PendingQtyDelta;
            public bool PendingTpChanged, PendingSlChanged;

        }

        private async Task EnsurePlaywrightAsync()
        {
            try { Directory.SetCurrentDirectory(AppContext.BaseDirectory); } catch { }
            var baseDir = AppContext.BaseDirectory;
            var driverDir = Path.Combine(baseDir, "pwdriver");
            Directory.CreateDirectory(driverDir);
            Environment.SetEnvironmentVariable("PLAYWRIGHT_DRIVER_PATH", driverDir);
            Environment.SetEnvironmentVariable("PLAYWRIGHT_BROWSERS_PATH", "0");
            Environment.SetEnvironmentVariable("PLAYWRIGHT_SKIP_VALIDATE_HOST_REQUIREMENTS", "true");

            try
            {
                _playwright = await Microsoft.Playwright.Playwright.CreateAsync();
                return;
            }
            catch (Microsoft.Playwright.PlaywrightException ex)
                when (ex.Message.Contains("Driver not found", StringComparison.OrdinalIgnoreCase) ||
                      ex.Message.Contains("missing required assets", StringComparison.OrdinalIgnoreCase))
            {
                var exit = Microsoft.Playwright.Program.Main(new[] { "install", "chromium" });
                if (exit != 0) throw;
                _playwright = await Microsoft.Playwright.Playwright.CreateAsync();
                return;
            }
        }

        public async Task Start(string url = "https://www.tradingview.com")
        {
            try { Directory.SetCurrentDirectory(AppContext.BaseDirectory); } catch { }
            await EnsurePlaywrightAsync();

            _browser = await _playwright.Chromium.LaunchAsync(new BrowserTypeLaunchOptions
            {
                Headless = false,
                Args = new[] { "--disable-blink-features=AutomationControlled" }
            });

            BrowserNewContextOptions options = new() { ViewportSize = null };
            if (File.Exists("storage.json")) { options.StorageStatePath = "storage.json"; }

            var context = await _browser.NewContextAsync(options);
            _page = await context.NewPageAsync();

            bool privateChannelFound = false;
            _page.WebSocket += (_, ws) =>
            {
                //Log.Information("Socket detectado: {Url}", ws.Url);
                if (ws.Url.Contains("pushstream.tradingview.com"))
                {
                    if (ws.Url.Contains("/private_", StringComparison.OrdinalIgnoreCase))
                    {
                        privateChannelFound = true;
                        AttachHandler(ws);
                    }
                    else
                    {
                        Log.Debug("[IGNORADO] {Url}", ws.Url);
                    }
                }
            };

            await _page.GotoAsync(url);
            await Task.Delay(5000);

            if (!privateChannelFound)
            {
                Log.Warning("Usuário não logado. Favor realizar login.");
                await _page.EvaluateAsync("alert('Usuário não logado. Favor realizar login e confirmar esta caixa para seguir com o processamento.')");
                while (!privateChannelFound) { await Task.Delay(1000); }
                await _page.Context.StorageStateAsync(new BrowserContextStorageStateOptions { Path = "storage.json" });
            }

            Log.Information("Monitorando página: {Url}", _page.Url);

            _page.WebSocket += (_, ws) =>
            {
                if (ws.Url.Contains("pushstream.tradingview.com"))
                    AttachHandler(ws);
            };

            foreach (var ctx in _browser.Contexts)
            {
                ctx.Page += (_, page) =>
                {
                    page.WebSocket += (_, ws) =>
                    {
                        Log.Information("Novo socket detectado (nova aba): {Url}", ws.Url);
                        if (ws.Url.Contains("pushstream.tradingview.com"))
                            AttachHandler(ws);
                    };
                };
            }

            _ = Task.Run(async () =>
            {
                while (true)
                {
                    await Task.Delay(FlushPeriodMs);
                    FlushPendingAdjustments();
                }
            });
        }

        private void AttachHandler(IWebSocket ws)
        {
            if (!ws.Url.Contains("/private_", StringComparison.OrdinalIgnoreCase))
            {
                Log.Debug("[IGNORADO] {Url}", ws.Url);
                return;
            }

            Log.Information("[WS-CONNECTED] {Url}", ws.Url);

            DateTime lastMessage = DateTime.UtcNow;
            ws.FrameReceived += (_, frame) =>
            {
                lastMessage = DateTime.UtcNow;
                var text = frame.Text;
                if (string.IsNullOrEmpty(text))
                    return;

                try
                {
                    using var doc = JsonDocument.Parse(text);

                    if (!doc.RootElement.TryGetProperty("text", out var textEl))
                        return;
                    
                    // se for string simples (ex.: "ping")
                    if (textEl.ValueKind == JsonValueKind.String)
                    {
                        Log.Debug("Mensagem ignorada (string): {Text}", textEl.GetString());
                        return;
                    }
                    else if (textEl.TryGetProperty("channel", out var ch) && ch.ValueKind == JsonValueKind.String)
                    {
                        var channel = ch.GetString();
                        if (channel != "trading")
                        {
                            Log.Debug("Canal ignorado (não-trading): ({Ch})", channel);
                            return;
                        }
                    }

                    if (textEl.ValueKind == JsonValueKind.Object && textEl.TryGetProperty("content", out var contentEl) && contentEl.TryGetProperty("m", out var mEl))
                    {
                        var type = mEl.GetString();
                        switch (type)
                        {
                            case "order_update":
                                //HandleOrder(contentEl);
                                break;
                            case "journal_update":
                                //Log.Information("📒 JOURNAL: {Data}", contentEl.ToString());
                                break;
                            case "position_update":
                                HandlePosition(contentEl);
                                break;
                            case "execution_update":
                                HandleExecution(contentEl);
                                break;
                            default:
                                Log.Debug("Outro evento: {Type} -> {Data}", type, contentEl.ToString());
                                break;
                        }
                    }
                }
                catch (Exception ex)
                {
                    Log.Error(ex, "Erro processando mensagem WS");
                }
            };

            ws.Close += async (_, _) =>
            {
                Log.Warning("[WS-CLOSED] {Url}", ws.Url);
                FlushPendingAdjustments(force: true);
                //await _page.ReloadAsync(new PageReloadOptions { WaitUntil = WaitUntilState.NetworkIdle });
            };
        }

        private void HandlePosition(JsonElement contentEl)
        {
            if (!TryExtractP(ref contentEl, out var pEl))
                return;

            long account = pEl.TryGetProperty("account", out var accEl) && accEl.ValueKind == JsonValueKind.Number
                ? accEl.GetInt64() : 0;

            string symbol = pEl.TryGetProperty("symbol", out var symEl) && symEl.ValueKind == JsonValueKind.String
                ? symEl.GetString() ?? "" : "";

            // aceita Number ou String
            decimal qty = GetDecOrZero(pEl, "qty");
            decimal avg = GetDecOrZero(pEl, "avg_price");
            decimal? sl = GetDecNullable(pEl, "sl");
            decimal? tp = GetDecNullable(pEl, "tp");

            string dir = qty > 0 ? "buy" : qty < 0 ? "sell" : "flat";

            var key = (account, symbol);

            lock (_sync)
            {
                _posicoesAtivas.TryGetValue(key, out var st);

                bool isFlatNow = qty == 0m;
                bool hadOpen = st is not null;

                // ---------- ABERTURA ----------
                if (!hadOpen && !isFlatNow)
                {
                    st = new PositionCycle
                    {
                        OpenTs = DateTime.UtcNow,
                        Dir = dir,
                        Qty = qty,
                        OpenPrice = avg,
                        LastAvg = avg,
                        SlStart = sl,
                        SlEnd = sl,
                        TpStart = tp,
                        TpEnd = tp
                    };
                    _posicoesAtivas[key] = st;
                    _earlyExec.Remove(key);

                    Log.Information("POSIÇÃO ABERTA {Dir} {QtyAbs} @ {Price} ({Symbol}) SL={Sl} TP={Tp}", dir, Math.Abs(qty), avg, symbol, sl, tp);

                    _pendingOpens[key] = new PendingOpen
                    {
                        Action = dir == "buy" ? 1 : 2,
                        Symbol = symbol,
                        Avg = avg,
                        Position = qty,
                        Tp = tp,
                        Sl = sl,
                        CreatedAtUtc = DateTime.UtcNow
                    };

                    _ = Task.Run(async () =>
                    {
                        await Task.Delay(EarlyExecGraceMs + 10);
                        FlushPendingAdjustments(force: true);
                    });

                    return;
                }

                // Sem posição e continua flat  ->  nada
                if (!hadOpen && isFlatNow)
                {
                    var key2 = (account, symbol);
                    EarlyExecAgg ? agg = null;
                    if (_earlyExec.TryGetValue(key2, out var a) && (DateTime.UtcNow - a.LastAtUtc).TotalMilliseconds <= EarlyExecGraceMs && a.QtyAbs > 0)
                    {
                        agg = a;
                        _earlyExec.Remove(key2);
                    }

                    if (agg is not null)
                    {
                        _ = EmitCallAsync(
                            action: 0,
                            ticker: symbol,
                            avg: agg.LastPrice,
                            position: 0m,
                            execQty: agg.QtyAbs,
                            tp: null,
                            sl: null
                        );
                        Log.Warning("FECHAMENTO SEM ESTADO LOCAL ({Symbol}) – consolidado com execs: qty={Qty}", symbol, agg.QtyAbs);
                    }
                    else
                    {
                        // Não houve execs “recentes” associados  ->  não emitir nada (evita close vazio)
                        Log.Debug("SNAPSHOT FLAT sem exec recente ({Symbol}) – nenhuma emissão.", symbol);
                    }

                    _pendingOpens.Remove(key2);
                    _pendingReverses.Remove(key2);
                    return;
                }

                // ---------- ABERTA: atualizações ou REVERSÃO ----------
                if (hadOpen && !isFlatNow)
                {
                    // REVERSÃO (mudança de sinal)
                    bool signChanged = Math.Sign(qty) != Math.Sign(st!.Qty);

                    if (signChanged && Math.Sign(qty) != 0)
                    {
                        var dur = DateTime.UtcNow - st.OpenTs;
                        var notesPrev = new List<string>();

                        if (st.PendingInfos.Count > 0)
                            notesPrev.AddRange(st.PendingInfos);
                        else
                        {
                            if (st.SlChanged) notesPrev.Add($"SL {st.SlStart} -> {st.SlEnd}");
                            if (st.TpCancelled) notesPrev.Add("TP cancelado");
                            else if (st.TpSetLater) notesPrev.Add($"TP definido {st.TpEnd}");
                            else if (st.TpChanged) notesPrev.Add($"TP {st.TpStart} -> {st.TpEnd}");
                        }

                        notesPrev = notesPrev.Distinct().ToList();

                        Log.Information("POSIÇÃO FECHADA ({Symbol}) ciclo {Dir} {QtyAbs} @ {Price} dur={Dur}{Notas}",
                            symbol, st.Dir, Math.Abs(st.Qty), st.OpenPrice, dur,
                            notesPrev.Count > 0 ? " | " + string.Join("; ", notesPrev) : "");

                        // abre novo ciclo
                        st = new PositionCycle
                        {
                            OpenTs = DateTime.UtcNow,
                            Dir = dir,
                            Qty = qty,
                            OpenPrice = avg,
                            LastAvg = avg,
                            SlStart = sl,
                            SlEnd = sl,
                            TpStart = tp,
                            TpEnd = tp
                        };
                        _posicoesAtivas[key] = st;
                        _earlyExec.Remove(key);

                        _pendingOpens.Remove(key);

                        Log.Information("POSIÇÃO ABERTA (reversão) {Dir} {QtyAbs} @ {Price} ({Symbol}) SL={Sl} TP={Tp}",
                            dir, Math.Abs(qty), avg, symbol, sl, tp);

                        _pendingOpens.Remove(key);
                        _pendingReverses[key] = new PendingReverse
                        {
                            Action = dir == "buy" ? 4 : 5,
                            Symbol = symbol,
                            Avg = avg,
                            Position = qty,
                            Tp = tp,
                            Sl = sl,
                            CreatedAtUtc = DateTime.UtcNow
                        };

                        _ = Task.Run(async () =>
                        {
                            await Task.Delay(EarlyExecGraceMs + 10);
                            FlushPendingAdjustments(force: true);
                        });

                        return;
                    }

                    // -------- AJUSTES EM TEMPO REAL (com debounce) --------
                    bool anyChange = false;
                    var prevQtyAbs = Math.Abs(st!.Qty);
                    var newQtyAbs = Math.Abs(qty);

                    if (qty != st.Qty)
                    {
                        st.PendingInfos.Add($"QTY {prevQtyAbs} -> {newQtyAbs}");
                        anyChange = true;
                    }

                    // média da posição pode mudar quando há execuções parciais/adicionais
                    if (avg != st.LastAvg)
                    {
                        st.PendingInfos.Add($"AVG {st.LastAvg} -> {avg}");
                        st.LastAvg = avg;
                        anyChange = true;
                    }

                    // SL
                    if (Diff(st.SlEnd, sl))
                    {
                        var oldSl = st.SlEnd;
                        st.SlChanged = true;
                        st.SlEnd = sl;
                        st.PendingInfos.Add($"SL {oldSl?.ToString() ?? "-"} -> {sl?.ToString() ?? "-"}");
                        st.PendingSlChanged = true;
                        anyChange = true;
                    }

                    // TP
                    if (Diff(st.TpEnd, tp))
                    {
                        var oldTp = st.TpEnd;
                        if (oldTp is not null && tp is null) { st.TpCancelled = true; st.PendingInfos.Add("TP cancelado"); }
                        else if (oldTp is null && tp is not null) { st.TpSetLater = true; st.PendingInfos.Add($"TP definido {tp}"); }
                        else { st.TpChanged = true; st.PendingInfos.Add($"TP {oldTp?.ToString() ?? "-"} -> {tp?.ToString() ?? "-"}"); }

                        st.TpEnd = tp;
                        st.PendingTpChanged = true;
                        anyChange = true;
                    }

                    if (anyChange && st.PendingSince is null) st.PendingSince = DateTime.UtcNow;

                    // magnitude pode mudar sem reversão
                    st.Qty = qty;
                    return;
                }

                // ---------- FECHAMENTO (flat) ----------
                if (hadOpen && isFlatNow)
                {
                    var dur = DateTime.UtcNow - st!.OpenTs;
                    var notes = new List<string>();
                    if (st.PendingInfos.Count > 0)
                        notes.AddRange(st.PendingInfos);
                    else
                    {
                        if (st.SlChanged) notes.Add($"SL {st.SlStart} -> {st.SlEnd}");
                        if (st.TpCancelled) notes.Add("TP cancelado");
                        else if (st.TpSetLater) notes.Add($"TP definido {st.TpEnd}");
                        else if (st.TpChanged) notes.Add($"TP {st.TpStart} -> {st.TpEnd}");
                    }
                    notes = notes.Distinct().ToList();

                    Log.Information("POSIÇÃO FECHADA ({Symbol}) ciclo {Dir} {QtyAbs} @ {Price} dur={Dur}{Notas}",
                        symbol, st.Dir, Math.Abs(st.Qty), st.OpenPrice, dur,
                        notes.Count > 0 ? " | " + string.Join("; ", notes) : "");

                    _ = EmitCallAsync(0, symbol, 0m, position: 0m, execQty: Math.Abs(st.Qty), tp: null, sl: null);

                    _posicoesAtivas.Remove(key);
                    _lastExecEmitAt.Remove(key);
                    _pendingOpens.Remove(key);
                    _pendingReverses.Remove(key);
                }
            }
        }

        private void HandleExecution(JsonElement contentEl)
        {
            if (!contentEl.TryGetProperty("accountId", out var accEl) || accEl.ValueKind != JsonValueKind.Number)
                return;
            long account = accEl.GetInt64();

            if (!contentEl.TryGetProperty("p", out var pEl) || pEl.ValueKind != JsonValueKind.Object)
                return;

            string symbol = pEl.TryGetProperty("symbol", out var symEl) && symEl.ValueKind == JsonValueKind.String ? symEl.GetString() ?? "" : "";
            string side = pEl.TryGetProperty("side", out var sideEl) && sideEl.ValueKind == JsonValueKind.String ? sideEl.GetString() ?? "" : "";
            decimal qty = GetDecOrZero(pEl, "qty");
            decimal price = GetDecOrZero(pEl, "price");

            var key = (account, symbol);
            int action = side == "buy" ? 1 : 2;

            decimal positionSnapshot = 0m, avgSnapshot = price;
            decimal? tp = null, sl = null;

            lock (_sync)
            {
                if (_posicoesAtivas.TryGetValue(key, out var st) && st is not null)
                {
                    positionSnapshot = st.Qty;
                    avgSnapshot = st.LastAvg;
                    tp = st.TpEnd; sl = st.SlEnd;

                    var now = DateTime.UtcNow;
                    if (_lastExecEmitAt.TryGetValue(key, out var last) &&
                        (now - last).TotalMilliseconds < 120)
                    {
                        return;
                    }

                    _ = EmitCallAsync(action, symbol, avgSnapshot, positionSnapshot, Math.Abs(qty), tp, sl);
                    _lastExecEmitAt[key] = DateTime.UtcNow;
                    _pendingOpens.Remove(key);
                    _pendingReverses.Remove(key);
                    return;
                }

                if (!_earlyExec.TryGetValue(key, out var agg))
                    _earlyExec[key] = agg = new EarlyExecAgg();

                agg.QtyAbs += Math.Abs(qty);
                agg.LastPrice = price;
                agg.LastAtUtc = DateTime.UtcNow;

                _pendingOpens.Remove(key);
                _pendingReverses.Remove(key);
            }
        }

        private void HandleOrder(JsonElement contentEl)
        {
            // 1) Normaliza/acha o "p"
            if (!TryExtractP(ref contentEl, out var pEl))
                return;

            // 2) Campos essenciais
            if (!TryParseOrderCore(pEl, out var id, out var type, out var price, out var status))
                return;

            // 3) Regra: ignorar "pending"
            if (status.Equals("pending", StringComparison.OrdinalIgnoreCase))
            {
                Log.Debug("Ordem ignorada (status pending): {Id}/{Type}", id, type);
                return;
            }

            // 4) tp/sl com presença
            var (hasTp, tp) = ReadDecimalField(pEl, "tp");
            var (hasSl, sl) = ReadDecimalField(pEl, "sl");

            // 5) Hash opcional (mensagem idêntica)
            string jsonAtual = contentEl.ValueKind == JsonValueKind.Object ? contentEl.ToString() : pEl.ToString();
            string hash = Convert.ToBase64String(System.Security.Cryptography.SHA1.HashData(System.Text.Encoding.UTF8.GetBytes(jsonAtual)));
            if (_hashesProcessados.Contains(hash))
                return;

            var key = new OrderKey(id, type);

            if (!_ultimoEstadoPorChave.TryGetValue(key, out var estadoAntigo))
            {
                if (pEl.TryGetProperty("oco", out var ocoEl) && ocoEl.ValueKind == JsonValueKind.String && !string.IsNullOrWhiteSpace(ocoEl.GetString()))
                {
                    Log.Debug("Ignorado como ORDEM NOVA ({Key}) por conter 'oco' != vazio: {Oco}", key, ocoEl.GetString());
                    _hashesProcessados.Add(hash);
                    return;
                }

                _ultimoEstadoPorChave[key] = new OrderState
                {
                    Price = price,
                    Status = status,
                    HasTp = hasTp,
                    Tp = tp,
                    HasSl = hasSl,
                    Sl = sl,
                    LastJson = jsonAtual
                };
                Log.Information("ORDEM NOVA ({Key}) -> {Data}", key, jsonAtual);
                _hashesProcessados.Add(hash);
                return;
            }

            bool mudouPreco = estadoAntigo.Price != price;
            bool mudouStatus = !string.Equals(estadoAntigo.Status, status, StringComparison.OrdinalIgnoreCase);
            bool mudouTp = (estadoAntigo.HasTp != hasTp) || (hasTp && estadoAntigo.Tp != tp);
            bool mudouSl = (estadoAntigo.HasSl != hasSl) || (hasSl && estadoAntigo.Sl != sl);

            if (!mudouPreco && !mudouStatus && !mudouTp && !mudouSl)
            {
                Log.Debug("Sem alteração ({Key}). Price={Price}, Status={Status}, HasTp={HasTp}, Tp={Tp}, HasSl={HasSl}, Sl={Sl}",
                          key, price, status, hasTp, tp, hasSl, sl);
                return;
            }

            Log.Information("ATUALIZAÇÃO ({Key}) " +
                            "Price: {OldP}->{NewP} | Status: {OldS}->{NewS} | " +
                            "TP: {OldHasTp}/{OldTp} -> {NewHasTp}/{NewTp} | " +
                            "SL: {OldHasSl}/{OldSl} -> {NewHasSl}/{NewSl}",
                key,
                estadoAntigo.Price, price,
                estadoAntigo.Status, status,
                estadoAntigo.HasTp, estadoAntigo.Tp, hasTp, tp,
                estadoAntigo.HasSl, estadoAntigo.Sl, hasSl, sl);

            // aplica novo estado (mantemos apenas 1 histórico por chave)
            estadoAntigo.Price = price;
            estadoAntigo.Status = status;
            estadoAntigo.HasTp = hasTp; estadoAntigo.Tp = tp;
            estadoAntigo.HasSl = hasSl; estadoAntigo.Sl = sl;
            estadoAntigo.LastJson = jsonAtual;
            _ultimoEstadoPorChave[key] = estadoAntigo;

            // (Opcional) ação específica para cancelamento
            if (status.Equals("cancelled", StringComparison.OrdinalIgnoreCase))
            {
                Log.Warning("ORDEM CANCELADA ({Key})", key);
                // _ultimoEstadoPorChave.Remove(key); // se preferir remover
            }

            _hashesProcessados.Add(hash);
        }

        private static bool TryExtractP(ref JsonElement root, out JsonElement pEl)
        {
            if (root.ValueKind == JsonValueKind.String)
            {
                var s = root.GetString();
                if (!string.IsNullOrWhiteSpace(s))
                {
                    using var doc = JsonDocument.Parse(s);
                    root = doc.RootElement.Clone();
                }
            }

            if (root.TryGetProperty("p", out pEl) && pEl.ValueKind == JsonValueKind.Object)
                return true;

            if (root.ValueKind == JsonValueKind.Object)
            {
                pEl = root;
                return true;
            }

            pEl = default;
            return false;
        }

        private static bool TryParseOrderCore(JsonElement pEl, out long id, out string type, out decimal price, out string status)
        {
            id = 0; type = ""; price = 0m; status = "";

            // id (número ou string)
            if (!pEl.TryGetProperty("id", out var idEl)) return false;
            if (idEl.ValueKind == JsonValueKind.Number)
            {
                if (!idEl.TryGetInt64(out id)) return false;
            }
            else if (idEl.ValueKind == JsonValueKind.String)
            {
                if (!long.TryParse(idEl.GetString(), out id)) return false;
            }
            else return false;

            // type
            if (!pEl.TryGetProperty("type", out var typeEl) || typeEl.ValueKind != JsonValueKind.String) return false;
            type = typeEl.GetString() ?? "";

            // price (número ou string)
            if (!pEl.TryGetProperty("price", out var priceEl)) return false;
            if (priceEl.ValueKind == JsonValueKind.Number)
            {
                if (!priceEl.TryGetDecimal(out price)) return false;
            }
            else if (priceEl.ValueKind == JsonValueKind.String)
            {
                if (!decimal.TryParse(priceEl.GetString(), System.Globalization.NumberStyles.Any,
                    System.Globalization.CultureInfo.InvariantCulture, out price)) return false;
            }
            else return false;

            // status (pode vir "pending", "filled", "cancelled", etc.)
            if (pEl.TryGetProperty("status", out var stEl) && stEl.ValueKind == JsonValueKind.String)
                status = stEl.GetString() ?? "";
            else
                status = "";

            return true;
        }

        private static (bool has, decimal value) ReadDecimalField(JsonElement obj, string name)
        {
            if (!obj.TryGetProperty(name, out var el))
                return (false, 0m);

            switch (el.ValueKind)
            {
                case JsonValueKind.Number:
                    if (el.TryGetDecimal(out var v)) return (true, v);
                    break;
                case JsonValueKind.String:
                    if (decimal.TryParse(el.GetString(), System.Globalization.NumberStyles.Any,
                                         System.Globalization.CultureInfo.InvariantCulture, out var vs))
                        return (true, vs);
                    break;
                case JsonValueKind.Null:
                    return (true, 0m);
            }
            return (true, 0m);
        }

        private static decimal? GetDecNullable(JsonElement obj, string name)
            {
                if (!obj.TryGetProperty(name, out var el)) return null;
                return el.ValueKind switch
                {
                    JsonValueKind.Number => el.TryGetDecimal(out var v) ? v : (decimal?)null,
                    JsonValueKind.String => decimal.TryParse(el.GetString(), NumberStyles.Any, CultureInfo.InvariantCulture, out var vs) ? vs : (decimal?)null,
                    JsonValueKind.Null => null,
                    _ => null
                };
            }

        private static decimal GetDecOrZero(JsonElement obj, string name)
        {
            if (!obj.TryGetProperty(name, out var el)) return 0m;
            return el.ValueKind switch
            {
                JsonValueKind.Number => el.TryGetDecimal(out var v) ? v : 0m,
                JsonValueKind.String => decimal.TryParse(el.GetString(), NumberStyles.Any, CultureInfo.InvariantCulture, out var vs) ? vs : 0m,
                _ => 0m
            };
        }

        private void FlushPendingAdjustments(bool force = false)
        {
            var toDebug = new List<AdjustLog>();
            var toCalls = new List<(string Symbol, string Dir, decimal QtySigned, decimal? Tp, decimal? Sl,
                                    long Account, bool SlChanged, bool TpChanged, decimal OpenPrice, decimal LastAvg)>();

            lock (_sync)
            {
                foreach (var kv in _posicoesAtivas)
                {
                    var ((account, symbol), st) = (kv.Key, kv.Value);

                    // qty via snapshot não conta como pendência que gere CALL
                    bool hasPend = st.PendingInfos.Count > 0 || st.PendingSlChanged || st.PendingTpChanged;
                    if (!hasPend) continue;

                    bool ready = force || (st.PendingSince is DateTime t && (DateTime.UtcNow - t).TotalMilliseconds >= AdjustDebounceMs);
                    if (!ready) continue;

                    if (st.PendingInfos.Count > 0)
                    {
                        var infos = st.PendingInfos.Distinct().ToList();
                        toDebug.Add(new AdjustLog(symbol, st.Dir, Math.Abs(st.Qty), string.Join("; ", infos)));
                    }

                    toCalls.Add((symbol, st.Dir, st.Qty, st.TpEnd, st.SlEnd, account,
                                 st.PendingSlChanged, st.PendingTpChanged, st.OpenPrice, st.LastAvg));

                    // limpa pendências do burst
                    st.PendingInfos.Clear();
                    st.PendingQtyDelta = 0;          // mantemos por compat, mas não usamos mais
                    st.PendingSlChanged = false;
                    st.PendingTpChanged = false;
                    st.PendingSince = null;
                }
            }

            // 1) DEBUG humano (mantém)
            foreach (var a in toDebug)
                Log.Information("POSIÇÃO AJUSTE ({Symbol}) {Dir} {QtyAbs} | {Infos}", a.Symbol, a.Dir, a.QtyAbs, a.Infos);

            // 2) SOMENTE TP/SL (sem qty)
            foreach (var c in toCalls)
            {
                if (c.TpChanged)
                    _ = EmitCallAsync(6, c.Symbol, c.LastAvg, position: c.QtySigned, execQty: 0m, tp: c.Tp, sl: c.Sl);

                if (c.SlChanged)
                {
                    var isBE = IsBreakEven(c.OpenPrice, c.Sl);
                    _ = EmitCallAsync(isBE ? 3 : 7, c.Symbol, c.LastAvg, position: c.QtySigned, execQty: 0m, tp: c.Tp, sl: c.Sl);
                }
            }

            // 3) ABERTURAS PENDENTES: emite só se não houve execution_update no intervalo
            List<PendingOpen> toEmitOpens = new();
            List<(long Account, string Symbol)> toDropOpens = new();

            List<PendingReverse> toEmitRevs = new();
            List<(long Account, string Symbol)> toDropRevs = new();

            lock (_sync)
            {
                foreach (var kv in _pendingOpens)
                {
                    var key = kv.Key;
                    var po = kv.Value;

                    // Se chegou execution_update muito perto do snapshot, cancelamos (evita duplicado).
                    if (_lastExecEmitAt.TryGetValue(key, out var lastExecAt))
                    {
                        var dtMs = (po.CreatedAtUtc - lastExecAt).Duration().TotalMilliseconds;
                        if (dtMs <= EarlyExecGraceMs)
                        {
                            toDropOpens.Add(key);
                            Log.Debug("Abertura pendente CANCELADA por exec recente ({Symbol}).", po.Symbol);
                            continue;
                        }
                    }

                    // Se a janela passou sem exec, emite agora a abertura do snapshot (qty=0).
                    if ((DateTime.UtcNow - po.CreatedAtUtc).TotalMilliseconds >= EarlyExecGraceMs)
                    {
                        toEmitOpens.Add(po);
                        toDropOpens.Add(key);
                    }
                }

                foreach (var kv in _pendingReverses)
                {
                    var key = kv.Key;
                    var pr = kv.Value;

                    // Se chegou execution_update muito perto, cancela reversão pendente
                    if (_lastExecEmitAt.TryGetValue(key, out var lastExecAt))
                    {
                        var dtMs = (pr.CreatedAtUtc - lastExecAt).Duration().TotalMilliseconds;
                        if (dtMs <= EarlyExecGraceMs)
                        {
                            toDropRevs.Add(key);
                            Log.Debug("Reversão pendente CANCELADA por exec recente ({0}).", pr.Symbol);
                            continue;
                        }
                    }

                    // Se passou a janela sem exec, emite a reversão do snapshot (qty=0 no exec)
                    if ((DateTime.UtcNow - pr.CreatedAtUtc).TotalMilliseconds >= EarlyExecGraceMs)
                    {
                        toEmitRevs.Add(pr);
                        toDropRevs.Add(key);
                    }
                }

                foreach (var k in toDropOpens) _pendingOpens.Remove(k);
                foreach (var k in toDropRevs) _pendingReverses.Remove(k);

            }

            foreach (var po in toEmitOpens)
            {
                _ = EmitCallAsync(po.Action, po.Symbol, po.Avg, position: po.Position, execQty: 0m, tp: po.Tp, sl: po.Sl);
            }

            foreach (var pr in toEmitRevs)
            {
                _ = EmitCallAsync(pr.Action, pr.Symbol, pr.Avg, position: pr.Position, execQty: 0m, tp: pr.Tp, sl: pr.Sl);
            }
        }

        private async Task EmitCallAsync(int action, string ticker, decimal avg, decimal position, decimal execQty, decimal? tp, decimal? sl)
        {
            var payload = new
            {
                message = new
                {
                    source = "COPY_TV",
                    action = action,
                    ticker = ticker,
                    close = avg,
                    position = position,
                    qty = execQty,
                    tickSize = 0,
                    takeProfit = tp ?? 0m,
                    stopLoss = sl ?? 0m,
                    infoTP = 0m,
                    infoSL = 0m,
                    invBol = 0,
                    timestamp = DateTime.UtcNow.ToString("yyyy-MM-ddTHH:mm:ss.fffZ")
                }
            };

            var json = JsonSerializer.Serialize(
                payload,
                new JsonSerializerOptions
                {
                    Encoder = JavaScriptEncoder.Create(UnicodeRanges.BasicLatin),
                    WriteIndented = false
                });

            const int maxAttempts = 2;
            for (int attempt = 1; attempt <= maxAttempts; attempt++)
            {
                using var content = new StringContent(json, Encoding.UTF8, "application/json");
                try
                {
                    var resp = await _http.PostAsync(ForwardUrl, content);
                    if (resp.IsSuccessStatusCode)
                    {
                        Log.Information("CALL SENT ({Status}): {Json}", (int)resp.StatusCode, json);
                        break;
                    }
                    else
                    {
                        Log.Warning("CALL FAILED ({Status}) attempt {Attempt}/{Max}: {Json}",
                            (int)resp.StatusCode, attempt, maxAttempts, json);

                        if (attempt == maxAttempts)
                            Log.Error("CALL GIVE-UP: {Json}", json);

                        await Task.Delay(250);
                    }
                }
                catch (Exception ex)
                {
                    Log.Error(ex, "CALL EXCEPTION attempt {Attempt}/{Max}: {Json}", attempt, maxAttempts, json);

                    if (attempt == maxAttempts)
                        Log.Error("CALL GIVE-UP (exception): {Json}", json);

                    await Task.Delay(300);
                }
            }
        }

    }

    public sealed class FetchMonitor
    {
        private IPlaywright? _pw;
        private IBrowser? _browser;
        private IBrowserContext? _context;
        private IPage? _page;

        private readonly FetchChannelMonitor _channel = new();

        private readonly Dictionary<string, (decimal SignedQty, decimal Avg)> _lastPosByInstr = new();
        private readonly Dictionary<string, List<OrderSummary>> _workingOrdersByInstr = new();
        private readonly Dictionary<string, CombinedState> _lastCombinedByInstr = new();
        private readonly Dictionary<string, DateTime> _suppressAdjustUntil = new();
        private readonly Dictionary<string, DateTime> _closingUntil = new(StringComparer.OrdinalIgnoreCase);

        private readonly HashSet<string> _seenExecIds = new();
        private readonly HashSet<string> _execPrimedInstr = new();

        private readonly object _gate = new();
        private long _startEpochSec;

        private sealed record OrderSummary(string Type, string Status, decimal? Limit, decimal? Stop, string Side);
        private sealed record CombinedState(decimal SignedQty, decimal Avg, decimal? Tp, decimal? Sl);

        //private static readonly HttpClient _http = new() { Timeout = TimeSpan.FromSeconds(7) };
        private static readonly SocketsHttpHandler _handlerFetch = new()
        {
            PooledConnectionLifetime = TimeSpan.FromMinutes(5),
            PooledConnectionIdleTimeout = TimeSpan.FromMinutes(2),
            MaxConnectionsPerServer = 8,
            ConnectTimeout = TimeSpan.FromSeconds(2),
            AutomaticDecompression = DecompressionMethods.GZip | DecompressionMethods.Deflate
        };
        private static readonly HttpClient _http = new(_handlerFetch)
        {
            Timeout = TimeSpan.FromSeconds(5)
        };

        private const string ForwardUrl = "https://copycapi-48479afb2dfb.herokuapp.com/forward";

        private const int SuppressAdjustMs = 500;
        private const int EarlyExecGraceMs = 250;
        private static bool IsBreakEven(decimal openPrice, decimal? sl) => sl.HasValue && sl.Value == openPrice;

        private sealed class EarlyExecAgg
        {
            public decimal QtyAbs;
            public decimal LastPrice;
            public int LastAction;
            public DateTime LastAtUtc;
            public bool TimerScheduled;
        }
        private readonly Dictionary<string, EarlyExecAgg> _earlyExec = new();
        private readonly Dictionary<string, DateTime> _lastCloseEmitAt = new();

        public async Task StartAsync(string url, CancellationToken ct = default)
        {
            PrepareEnv();
            await EnsurePlaywrightAsync();

            _ = Task.Run(async () =>
            {
                try
                {
                    using var req = new HttpRequestMessage(HttpMethod.Head, ForwardUrl);
                    using var resp = await _http.SendAsync(req);
                    Log.Debug("FORWARD warm-up (socket) status={0}", (int)resp.StatusCode);
                }
                catch (Exception ex)
                {
                    Log.Warning(ex, "FORWARD warm-up (socket) falhou (segue normal)");
                }
            });

            _startEpochSec = DateTimeOffset.UtcNow.ToUnixTimeSeconds();
            Log.Information("FETCH start epoch (UTC): {Epoch} ({Iso})",
                _startEpochSec, DateTimeOffset.FromUnixTimeSeconds(_startEpochSec).UtcDateTime.ToString("yyyy-MM-ddTHH:mm:ssZ"));

            _browser = await _pw!.Chromium.LaunchAsync(new BrowserTypeLaunchOptions
            {
                Headless = false,
                Args = new[] { "--disable-blink-features=AutomationControlled" }
            });

            var ctxOptions = new BrowserNewContextOptions { ViewportSize = null };
            if (File.Exists("storage.json")) ctxOptions.StorageStatePath = "storage.json";
            _context = await _browser.NewContextAsync(ctxOptions);
            _page = await _context.NewPageAsync();

            _browser.Disconnected += (_, __) => Log.Warning("BROWSER DISCONNECTED");
            _page.Close += (_, __) => Log.Warning("PAGE CLOSED");
            _page.PageError += (_, err) => Log.Error("PAGE ERROR: {Err}", err);

            _channel.Attach(_page);
            _channel.OnChanged += async (key, json) =>
            {
                try
                {
                    if (json.ValueKind != JsonValueKind.Object) return;
                    if (!json.TryGetProperty("d", out var arr) || arr.ValueKind != JsonValueKind.Array) return;

                    // POSITIONS: atualiza snapshot
                    if (key.Contains("/positions?", StringComparison.OrdinalIgnoreCase))
                    {
                        var seenNow = new HashSet<string>(StringComparer.OrdinalIgnoreCase);
                        foreach (var el in arr.EnumerateArray())
                        {
                            var instr = el.GetProperty("instrument").GetString() ?? "";
                            if (string.IsNullOrWhiteSpace(instr)) continue;
                            seenNow.Add(instr);

                            decimal qty = 0m, avg = 0m;
                            if (el.TryGetProperty("qty", out var pQty) && pQty.ValueKind == JsonValueKind.Number) pQty.TryGetDecimal(out qty);
                            if (el.TryGetProperty("avgPrice", out var pAvg) && pAvg.ValueKind == JsonValueKind.Number) pAvg.TryGetDecimal(out avg);

                            var side = el.TryGetProperty("side", out var pSide) && pSide.ValueKind == JsonValueKind.String ? (pSide.GetString() ?? "") : "";
                            var signed = side.Equals("buy", StringComparison.OrdinalIgnoreCase) ? qty
                                       : side.Equals("sell", StringComparison.OrdinalIgnoreCase) ? -qty : 0m;

                            lock (_gate) _lastPosByInstr[instr] = (signed, avg);
                            RecomputeCombined(instr);
                        }

                        // 2) Descobre quem FECHOU (sumiu do array) e limpa caches principais
                        List<(string Instr, decimal PrevQty)> closed;
                        lock (_gate)
                        {
                            closed = _lastPosByInstr
                                .Where(kv => !seenNow.Contains(kv.Key) && kv.Value.SignedQty != 0m)
                                .Select(kv => (kv.Key, kv.Value.SignedQty))
                                .ToList();

                            foreach (var (instr, _) in closed)
                            {
                                _lastPosByInstr.Remove(instr);
                                _lastCombinedByInstr.Remove(instr);
                                _workingOrdersByInstr.Remove(instr);
                            }
                        }

                        // 3) Emite a ZERAGEM; AQUI entra o lock(_gate) para limpar o agregador de execuções
                        foreach (var (instr, prevSignedQty) in closed)
                        {
                            var execAbs = Math.Abs(prevSignedQty);
                            lock (_gate)
                            {
                                _closingUntil[instr] = DateTime.UtcNow.AddMilliseconds(SuppressAdjustMs);
                                _earlyExec.Remove(instr);
                                _suppressAdjustUntil[instr] = DateTime.UtcNow.AddMilliseconds(SuppressAdjustMs);
                            }

                            await EmitCallAsync(action: 0, symbol: instr, avg: 0m, position: 0m, execQty: execAbs, tp: null, sl: null);
                            Log.Information("CLOSED (fetch) {Instr}: emitted action=0 qty={QtyAbs}", instr, execAbs);
                        }

                        return;
                    }

                    if (key.Contains("/orders?", StringComparison.OrdinalIgnoreCase))
                    {
                        // Coleta ORDENS working por instrumento
                        var map = new Dictionary<string, List<OrderSummary>>(StringComparer.OrdinalIgnoreCase);

                        foreach (var el in arr.EnumerateArray())
                        {
                            if (!(el.TryGetProperty("status", out var pSt) && pSt.ValueKind == JsonValueKind.String &&
                                  string.Equals(pSt.GetString(), "working", StringComparison.OrdinalIgnoreCase)))
                                continue;

                            var instr = el.TryGetProperty("instrument", out var pInstr) && pInstr.ValueKind == JsonValueKind.String ? (pInstr.GetString() ?? "") : "";
                            if (string.IsNullOrWhiteSpace(instr)) continue;

                            var type = el.TryGetProperty("type", out var pType) && pType.ValueKind == JsonValueKind.String ? (pType.GetString() ?? "") : "";
                            var side = el.TryGetProperty("side", out var pSide) && pSide.ValueKind == JsonValueKind.String ? (pSide.GetString() ?? "") : "";

                            // Preço: tenta limitPrice / stopPrice / price
                            decimal? limit = null, stop = null;
                            if (el.TryGetProperty("limitPrice", out var pLim) && pLim.ValueKind == JsonValueKind.Number) { pLim.TryGetDecimal(out var v); limit = v; }
                            if (el.TryGetProperty("stopPrice", out var pSto) && pSto.ValueKind == JsonValueKind.Number) { pSto.TryGetDecimal(out var v); stop = v; }
                            if (limit is null && stop is null && el.TryGetProperty("price", out var pPx) && pPx.ValueKind == JsonValueKind.Number)
                            { pPx.TryGetDecimal(out var v); if (string.Equals(type, "limit", StringComparison.OrdinalIgnoreCase)) limit = v; else stop = v; }

                            var o = new OrderSummary(type, "working", limit, stop, side);
                            (map.TryGetValue(instr, out var list) ? list : map[instr] = new List<OrderSummary>()).Add(o);
                        }

                        Dictionary<string, List<OrderSummary>> prev;
                        lock (_gate) prev = _workingOrdersByInstr.ToDictionary(kv => kv.Key, kv => kv.Value, StringComparer.OrdinalIgnoreCase);
                        lock (_gate) _workingOrdersByInstr.Clear();
                        foreach (var kv in map) _workingOrdersByInstr[kv.Key] = kv.Value;

                        List<string> toCheck;
                        lock (_gate) toCheck = _lastPosByInstr.Keys.ToList();
                        foreach (var instr in toCheck) RecomputeCombined(instr);

                        foreach (var kv in prev)
                        {
                            var instr = kv.Key;
                            var hadWorking = kv.Value is { Count: > 0 };
                            var hasWorkingNow = map.TryGetValue(instr, out var nowList) && nowList.Count > 0;

                            (decimal SignedQty, decimal Avg) pos;
                            lock (_gate) _lastPosByInstr.TryGetValue(instr, out pos);

                            if (hadWorking && !hasWorkingNow && pos.SignedQty != 0m)
                            {
                                //lock (_gate) _suppressAdjustUntil[instr] = DateTime.UtcNow.AddMilliseconds(SuppressAdjustMs);
                                lock (_gate)
                                {
                                    if (!_suppressAdjustUntil.TryGetValue(instr, out var until) || DateTime.UtcNow >= until)
                                        _suppressAdjustUntil[instr] = DateTime.UtcNow.AddMilliseconds(SuppressAdjustMs);
                                    _closingUntil[instr] = DateTime.UtcNow.AddMilliseconds(SuppressAdjustMs);
                                }
                                Log.Debug("SUPPRESS adjust for {Instr} (lost working orders w/ open position)", instr);
                            }
                        }

                        return;
                    }

                    // EXECUTIONS: envia só as novas (respostas acumuladas)
                    if (key.Contains("/executions?", StringComparison.OrdinalIgnoreCase))
                    {
                        var batch = new List<JsonElement>();
                        string? batchInstr = null;

                        foreach (var el in arr.EnumerateArray())
                        {
                            batch.Add(el);
                            if (batchInstr is null &&
                                el.TryGetProperty("instrument", out var pInstr) &&
                                pInstr.ValueKind == JsonValueKind.String)
                            {
                                batchInstr = pInstr.GetString();
                            }
                        }

                        if (!string.IsNullOrWhiteSpace(batchInstr))
                        {
                            bool needPrime;
                            lock (_gate) needPrime = _execPrimedInstr.Add(batchInstr);
                            if (needPrime)
                            {
                                var toProcessNow = new List<JsonElement>();

                                lock (_gate)
                                {
                                    foreach (var el in batch)
                                    {
                                        long? tSec = null;
                                        if (el.TryGetProperty("time", out var pTime) && pTime.ValueKind == JsonValueKind.Number)
                                        {
                                            if (pTime.TryGetInt64(out var l)) tSec = l;
                                            else tSec = (long)Math.Floor(pTime.GetDouble());
                                        }

                                        string? id = null;
                                        if (el.TryGetProperty("id", out var pId))
                                        {
                                            if (pId.ValueKind == JsonValueKind.String) id = pId.GetString();
                                            else if (pId.ValueKind == JsonValueKind.Number && pId.TryGetInt64(out var li))
                                                id = li.ToString(System.Globalization.CultureInfo.InvariantCulture);
                                        }

                                        if (tSec.HasValue && tSec.Value < _startEpochSec && !string.IsNullOrWhiteSpace(id))
                                        {
                                            _seenExecIds.Add(id);
                                        }
                                        else
                                        {
                                            toProcessNow.Add(el);
                                        }
                                    }
                                }
                                Log.Debug("Primed executions for {Instr} with {Count} existing ids.",
                                                   batchInstr, batch.Count);

                                batch = toProcessNow;
                            }
                        }

                        foreach (var el in batch)
                        {
                            //var id = el.TryGetProperty("id", out var pId) && pId.ValueKind == JsonValueKind.String ? (pId.GetString() ?? "") : "";
                            //if (string.IsNullOrWhiteSpace(id)) continue;
                            string? id = null;
                            if (el.TryGetProperty("id", out var pId))
                            {
                                if (pId.ValueKind == JsonValueKind.String) id = pId.GetString();
                                else if (pId.ValueKind == JsonValueKind.Number && pId.TryGetInt64(out var li))
                                    id = li.ToString(System.Globalization.CultureInfo.InvariantCulture);
                            }
                            if (string.IsNullOrWhiteSpace(id)) continue;

                            long? tSec = null;
                            if (el.TryGetProperty("time", out var pTime) && pTime.ValueKind == JsonValueKind.Number)
                            {
                                if (pTime.TryGetInt64(out var l)) tSec = l;
                                else tSec = (long)Math.Floor(pTime.GetDouble());
                            }
                            if (tSec.HasValue && tSec.Value < _startEpochSec)
                            {
                                lock (_gate) _seenExecIds.Add(id);
                                continue;
                            }

                            bool isNew;
                            lock (_gate) isNew = _seenExecIds.Add(id);
                            if (!isNew) continue;

                            var instr = el.TryGetProperty("instrument", out var pI) && pI.ValueKind == JsonValueKind.String ? (pI.GetString() ?? "") : "";
                            if (string.IsNullOrWhiteSpace(instr)) continue;

                            decimal price = 0m, qty = 0m;
                            if (el.TryGetProperty("price", out var pPx) && pPx.ValueKind == JsonValueKind.Number) pPx.TryGetDecimal(out price);
                            if (el.TryGetProperty("qty", out var pQty) && pQty.ValueKind == JsonValueKind.Number) pQty.TryGetDecimal(out qty);

                            var side = el.TryGetProperty("side", out var pSide) && pSide.ValueKind == JsonValueKind.String ? (pSide.GetString() ?? "") : "";
                            int action = side.Equals("buy", StringComparison.OrdinalIgnoreCase) ? 1 : 2;

                            // snapshot atual de posição
                            (decimal SignedQty, decimal Avg) snap;
                            bool hasOpen;
                            lock (_gate)
                                hasOpen = _lastPosByInstr.TryGetValue(instr, out snap) && snap.SignedQty != 0m;

                            if (hasOpen)
                            {
                                var positionSigned = snap.SignedQty;
                                var avgSnapshot = snap.Avg == 0m ? price : snap.Avg;

                                bool hasWorkingNow;
                                lock (_gate) hasWorkingNow = _workingOrdersByInstr.TryGetValue(instr, out var os) && os is { Count: > 0 };

                                if (!hasWorkingNow)
                                {
                                    await Task.Delay(60);
                                    lock (_gate) hasWorkingNow = _workingOrdersByInstr.TryGetValue(instr, out var os) && os is { Count: > 0 };
                                }

                                var (tp, sl) = GetTpSlFor(instr, positionSigned, avgSnapshot);
                                _ = EmitCallAsync(action, instr, avgSnapshot, positionSigned, Math.Abs(qty), tp, sl);

                                lock (_gate) _earlyExec.Remove(instr);
                                continue;
                            }

                            // ---- NÃO há posição aberta: agrega e decide após um pequeno delay ----
                            EarlyExecAgg agg;
                            bool schedule = false;
                            lock (_gate)
                            {
                                if (!_earlyExec.TryGetValue(instr, out agg))
                                {
                                    agg = new EarlyExecAgg();
                                    _earlyExec[instr] = agg;
                                    agg.TimerScheduled = false;
                                }
                                agg.QtyAbs += Math.Abs(qty);
                                agg.LastPrice = price;
                                agg.LastAction = action;
                                agg.LastAtUtc = DateTime.UtcNow;

                                if (!agg.TimerScheduled)
                                {
                                    agg.TimerScheduled = true;
                                    schedule = true;
                                }
                            }

                            if (schedule)
                            {
                                // espera janela para decidir entre “fechamento” (descarta) vs “abertura” (emite)
                                _ = Task.Run(async () =>
                                {
                                    await Task.Delay(EarlyExecGraceMs);
                                    decimal emitQty = 0m, emitAvg = 0m, positionSigned = 0m;
                                    int emitAction = 0;
                                    string sInstr = instr;
                                    bool shouldEmit = false;

                                    lock (_gate)
                                    {
                                        // Se após o grace a posição ABRIU/segue ≠0 -> emite exec agregada
                                        if (_lastPosByInstr.TryGetValue(sInstr, out var snap2) && snap2.SignedQty != 0m
                                            && _earlyExec.TryGetValue(sInstr, out var ag))
                                        {
                                            emitQty = ag.QtyAbs;
                                            emitAction = ag.LastAction;
                                            positionSigned = snap2.SignedQty;
                                            emitAvg = snap2.Avg == 0m ? ag.LastPrice : snap2.Avg;
                                            shouldEmit = true;
                                        }
                                        // Caso contrário (continua flat → fechamento já foi/será sinalizado por action=0) -> descarta
                                        _earlyExec.Remove(sInstr);
                                    }

                                    if (shouldEmit)
                                    {
                                        var (tp2, sl2) = GetTpSlFor(sInstr, positionSigned, emitAvg);
                                        _ = EmitCallAsync(emitAction, sInstr, emitAvg, positionSigned, emitQty, tp2, sl2);
                                    }
                                });
                            }
                        }
                        return;
                    }

                }
                catch (Exception ex)
                {
                    Log.Error(ex, "Erro no OnChanged (FetchMonitor)");
                }
            };

            await _page.GotoAsync(url);
            Log.Information("Fetch monitor ativo em {Url}", _page.Url);

            try { await Task.Delay(Timeout.Infinite, ct); } catch (TaskCanceledException) { }
        }

        public async Task StopAsync()
        {
            if (_page is not null) await _page.CloseAsync();
            if (_context is not null) await _context.CloseAsync();
            if (_browser is not null) await _browser.CloseAsync();
            _pw?.Dispose();
        }

        private static void PrepareEnv()
        {
            Environment.SetEnvironmentVariable("PLAYWRIGHT_BROWSERS_PATH", "0");
            Environment.SetEnvironmentVariable("PLAYWRIGHT_SKIP_VALIDATE_HOST_REQUIREMENTS", "true");
        }

        private async Task EnsurePlaywrightAsync()
        {
            try { _pw = await Playwright.CreateAsync(); }
            catch (PlaywrightException ex)
                when (ex.Message.Contains("Driver not found", StringComparison.OrdinalIgnoreCase) ||
                      ex.Message.Contains("missing required assets", StringComparison.OrdinalIgnoreCase))
            {
                var exit = Microsoft.Playwright.Program.Main(new[] { "install", "chromium" });
                if (exit != 0) throw;
                _pw = await Playwright.CreateAsync();
            }
        }

        private static async Task EmitCallAsync(int action, string symbol, decimal avg, decimal position, decimal execQty, decimal? tp, decimal? sl)
        {
            var payload = new
            {
                message = new
                {
                    source = "COPY_TV",
                    action,
                    ticker = symbol,
                    close = avg,
                    position,
                    qty = execQty,
                    tickSize = 0,
                    takeProfit = tp ?? 0m,
                    stopLoss = sl ?? 0m,
                    infoTP = 0m,
                    infoSL = 0m,
                    invBol = 0,
                    timestamp = DateTime.UtcNow.ToString("yyyy-MM-ddTHH:mm:ss.fffZ")
                }
            };

            var json = JsonSerializer.Serialize(
                payload,
                new JsonSerializerOptions
                {
                    Encoder = JavaScriptEncoder.Create(UnicodeRanges.BasicLatin),
                    WriteIndented = false
                });

            const int maxAttempts = 2;
            for (int attempt = 1; attempt <= maxAttempts; attempt++)
            {
                using var content = new StringContent(json, Encoding.UTF8, "application/json");
                try
                {
                    var resp = await _http.PostAsync(ForwardUrl, content);
                    if (resp.IsSuccessStatusCode)
                    {
                        Log.Information("CALL SENT ({Status}): {Json}", (int)resp.StatusCode, json);
                        break;
                    }
                    else
                    {
                        Log.Warning("CALL FAILED ({Status}) attempt {Attempt}/{Max}: {Json}",
                            (int)resp.StatusCode, attempt, maxAttempts, json);
                        if (attempt == maxAttempts) Log.Error("CALL GIVE-UP: {Json}", json);
                        await Task.Delay(250);
                    }
                }
                catch (Exception ex)
                {
                    Log.Error(ex, "CALL EXCEPTION attempt {Attempt}/{Max}: {Json}", attempt, maxAttempts, json);
                    if (attempt == maxAttempts) Log.Error("CALL GIVE-UP (exception): {Json}", json);
                    await Task.Delay(300);
                }
            }
        }

        private void RecomputeCombined(string instr)
        {
            (decimal SignedQty, decimal Avg) pos;
            CombinedState? old;
            List<OrderSummary>? ords;

            lock (_gate)
            {
                _lastPosByInstr.TryGetValue(instr, out pos);
                _workingOrdersByInstr.TryGetValue(instr, out ords);
                _lastCombinedByInstr.TryGetValue(instr, out old);
            }
            if (pos == default) return;

            var (tp, sl) = GetTpSlFor(instr, pos.SignedQty, pos.Avg, ords);
            var combined = new CombinedState(pos.SignedQty, pos.Avg, tp, sl);

            bool tpChanged = old is CombinedState o1 && o1.Tp != combined.Tp;
            bool slChanged = old is CombinedState o2 && o2.Sl != combined.Sl;
            bool positionChanged = old is CombinedState o3 && o3.SignedQty != combined.SignedQty;

            lock (_gate) _lastCombinedByInstr[instr] = combined;
            bool closingNow;
            lock (_gate)
                closingNow = _closingUntil.TryGetValue(instr, out var closeUntil) && DateTime.UtcNow < closeUntil;


            if (old is null && combined.SignedQty != 0m)
            {
                int action = combined.SignedQty > 0 ? 1 : 2;
                decimal qty = Math.Abs(combined.SignedQty);
                _ = EmitCallAsync(action, instr, combined.Avg, combined.SignedQty, qty, combined.Tp, combined.Sl);

                lock (_gate) _lastCombinedByInstr[instr] = combined;

                if (combined.SignedQty != 0m && (combined.Tp is not null || combined.Sl is not null))
                {
                    lock (_gate) _closingUntil.Remove(instr);
                }
            }

            bool tpSlBothRemoved = combined.SignedQty != 0m
                                   && combined.Tp is null && combined.Sl is null
                                   && ((old?.Tp is not null) || (old?.Sl is not null));

            if (tpSlBothRemoved)
            {
                lock (_gate)
                {
                    _suppressAdjustUntil[instr] = DateTime.UtcNow.AddMilliseconds(SuppressAdjustMs);
                    _closingUntil[instr] = DateTime.UtcNow.AddMilliseconds(SuppressAdjustMs);
                }
                Log.Debug("SUPPRESS & CLOSE-GUARD {Instr}: TP/SL removidos com posição aberta.", instr);
            }

            if (combined.SignedQty != 0m && (combined.Tp is not null || combined.Sl is not null))
            {
                lock (_gate) _closingUntil.Remove(instr);
            }

            bool suppressNow = false;
            lock (_gate)
            {
                if (_suppressAdjustUntil.TryGetValue(instr, out var until) && DateTime.UtcNow < until)
                    suppressNow = true;
            }

            if (positionChanged && combined.SignedQty != 0m)
            {
                int action = combined.SignedQty > 0 ? 1 : 2;
                decimal execQty = Math.Abs(combined.SignedQty - (old?.SignedQty ?? 0m));
                if (execQty > 0)
                {
                    _ = EmitCallAsync(action, instr, combined.Avg, position: combined.SignedQty, execQty: execQty, tp: combined.Tp, sl: combined.Sl);
                }
            }

            if (!suppressNow && !closingNow && tpChanged)
                _ = EmitCallAsync(6, instr, pos.Avg, position: pos.SignedQty, execQty: 0m, tp: combined.Tp, sl: combined.Sl);

            if (!suppressNow && !closingNow && slChanged)
            {
                var isBE = IsBreakEven(pos.Avg, combined.Sl);
                _ = EmitCallAsync(isBE ? 3 : 7, instr, pos.Avg, position: pos.SignedQty, execQty: 0m, tp: combined.Tp, sl: combined.Sl);
            }

            Log.Information("COMBINED {Instr}: pos={Pos} avg={Avg} tp={Tp} sl={Sl}",
                instr, pos.SignedQty, pos.Avg, combined.Tp?.ToString() ?? "-", combined.Sl?.ToString() ?? "-");
        }

        private (decimal? Tp, decimal? Sl) GetTpSlFor(string instr, decimal signedQty, decimal avg, List<OrderSummary>? ords = null)
        {
            if (ords == null)
            {
                lock (_gate) _workingOrdersByInstr.TryGetValue(instr, out ords);
            }
            if (ords is null || ords.Count == 0) return (null, null);

            string wantSide = signedQty > 0 ? "sell" : signedQty < 0 ? "buy" : "";
            if (!string.IsNullOrEmpty(wantSide))
                ords = ords.Where(o => string.Equals(o.Side, wantSide, StringComparison.OrdinalIgnoreCase)).ToList();

            var limits = ords.Where(o => string.Equals(o.Type, "limit", StringComparison.OrdinalIgnoreCase) && o.Limit.HasValue)
                             .Select(o => o.Limit!.Value).ToList();
            var stops = ords.Where(o => string.Equals(o.Type, "stop", StringComparison.OrdinalIgnoreCase) && o.Stop.HasValue)
                             .Select(o => o.Stop!.Value).ToList();

            decimal? tp = null, sl = null;

            if (signedQty > 0) // LONG
            {
                var tpCandidates = limits.Where(p => p >= avg);
                if (!tpCandidates.Any() && limits.Count > 0) tpCandidates = new List<decimal> { limits.Max() };
                var slCandidates = stops.Where(p => p < avg);
                if (!slCandidates.Any() && stops.Count > 0) slCandidates = new List<decimal> { stops.Min() };

                tp = tpCandidates.Any() ? tpCandidates.Min() : (decimal?)null;
                sl = slCandidates.Any() ? slCandidates.Max() : (decimal?)null;
            }
            else if (signedQty < 0) // SHORT
            {
                var tpCandidates = limits.Where(p => p <= avg);
                if (!tpCandidates.Any() && limits.Count > 0) tpCandidates = new List<decimal> { limits.Min() };
                var slCandidates = stops.Where(p => p > avg);
                if (!slCandidates.Any() && stops.Count > 0) slCandidates = new List<decimal> { stops.Max() };

                tp = tpCandidates.Any() ? tpCandidates.Max() : (decimal?)null;
                sl = slCandidates.Any() ? slCandidates.Min() : (decimal?)null;
            }

            return (tp, sl);
        }
    }

    public sealed class FetchChannelMonitor
    {
        private static readonly Regex OrdersRx = new(@"(^|/)(orders\?)", RegexOptions.IgnoreCase | RegexOptions.Compiled);
        private static readonly Regex PositionsRx = new(@"(^|/)(positions\?)", RegexOptions.IgnoreCase | RegexOptions.Compiled);
        private static readonly Regex ExecutionsRx = new(@"(^|/)(executions\?)", RegexOptions.IgnoreCase | RegexOptions.Compiled);

        private readonly Dictionary<string, string> _lastBodyHashByKey = new();
        public event Action<string, JsonElement>? OnChanged;

        private readonly object _gate = new();
        private int _attachedOnce = 0;

        private static bool IsTarget(string url)
            => OrdersRx.IsMatch(url) || PositionsRx.IsMatch(url) || ExecutionsRx.IsMatch(url);

        public void Attach(IPage page)
        {
            // garante que não vamos anexar duas vezes no mesmo monitor
            if (Interlocked.Exchange(ref _attachedOnce, 1) == 1)
            {
                Log.Debug("FetchChannelMonitor.Attach() já foi chamado; ignorando segundo attach.");
                return;
            }

            page.Response += async (_, resp) =>
            {
                try
                {
                    var url = resp.Url ?? "";
                    if (!IsTarget(url)) return;

                    var bytes = await resp.BodyAsync();
                    if (bytes is null || bytes.Length == 0) return;

                    var key = KeyFromUrl(url);

                    JsonElement json;
                    string bodyForHash;
                    try
                    {
                        using var doc = JsonDocument.Parse(bytes);
                        json = doc.RootElement.Clone();
                        bodyForHash = BuildHashBasis(key, json, bytes);
                    }
                    catch
                    {
                        json = default;
                        bodyForHash = Encoding.UTF8.GetString(bytes);
                    }

                    var hash = Sha1(bodyForHash);

                    bool changed;
                    lock (_gate)
                    {
                        changed = !_lastBodyHashByKey.TryGetValue(key, out var oldHash) || oldHash != hash;
                        if (changed) _lastBodyHashByKey[key] = hash;
                    }
                    if (!changed) return;

                    //Log.Information("FETCH CHANGE detectado em {Key} (HTTP {Status})", key, (int)resp.Status);

                    var raw = Encoding.UTF8.GetString(bytes);
                    //Log.Information("[FETCH RAW] {Key}{NL}{Raw}", key, Environment.NewLine, raw);

                    // opcional: mantém o evento
                    OnChanged?.Invoke(key, json);
                }
                catch (Exception ex)
                {
                    Log.Error(ex, "Erro ao processar resposta de fetch");
                }
            };
        }

        
        private static string BuildSemanticBodyForPositions(JsonElement root)
        {
            // Espera: { "s":"ok", "d":[ { instrument, qty, side, avgPrice, ... }, ... ] }
            if (root.ValueKind != JsonValueKind.Object || !root.TryGetProperty("d", out var arr) || arr.ValueKind != JsonValueKind.Array)
                return root.GetRawText();

            var list = new List<string>();
            foreach (var el in arr.EnumerateArray())
            {
                var instr = el.TryGetProperty("instrument", out var pInstr) && pInstr.ValueKind == JsonValueKind.String ? (pInstr.GetString() ?? "") : "";
                decimal qty = 0m, avg = 0m;
                if (el.TryGetProperty("qty", out var pQty) && pQty.ValueKind == JsonValueKind.Number) pQty.TryGetDecimal(out qty);
                if (el.TryGetProperty("avgPrice", out var pAvg) && pAvg.ValueKind == JsonValueKind.Number) pAvg.TryGetDecimal(out avg);
                var side = el.TryGetProperty("side", out var pSide) && pSide.ValueKind == JsonValueKind.String ? (pSide.GetString() ?? "") : "";

                // string canônica (ignora unrealizedPl)
                list.Add($"{instr}|{side}|{qty:0.########}|{avg:0.########}");
            }
            list.Sort(StringComparer.Ordinal); // ordem estável
            return string.Join(";", list);
        }

        private static string BuildHashBasis(string key, JsonElement json, byte[] rawBytes)
        {
            // Semântica só para positions? (pra já resolver o seu caso); o resto segue bruto
            if (key.Contains("/positions?", StringComparison.OrdinalIgnoreCase))
                return BuildSemanticBodyForPositions(json);

            // Para outros endpoints, usa o corpo bruto
            return Encoding.UTF8.GetString(rawBytes);
        }

        private static string KeyFromUrl(string url)
        {
            try
            {
                var u = new Uri(url);
                return $"{u.Host}{u.AbsolutePath}?{u.Query.TrimStart('?')}";
            }
            catch
            {
                return url;
            }
        }

        private static string Sha1(string s)
        {
            var bytes = Encoding.UTF8.GetBytes(s);
            var hash = SHA1.HashData(bytes);
            return Convert.ToHexString(hash);
        }
    }
}
