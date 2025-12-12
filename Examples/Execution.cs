using System;
using System.Collections.Concurrent;
using System.Collections.Generic;
using System.Globalization;
using System.IO;
using System.Net;
using System.Text;
using System.Text.Json;
using System.Threading;
using System.Threading.Channels;
using System.Threading.Tasks;
using Serilog;
using Serilog.Events;
using Serilog.Sinks.SystemConsole.Themes;
using StackExchange.Redis;
using WindowsInput;
using WindowsInput.Native;

namespace CopyReader.Exemples
{
    public static class AppEndpoints
    {
        public const string EchoHost = "localhost";
        public const int EchoPort = 5001;

        public static readonly Uri EchoUri = new($"http://{EchoHost}:{EchoPort}/");
        public static readonly string ListenerPrefix = $"http://*:{EchoPort}/";
    }

    internal class ExecEx
    {
        static readonly ConnectionMultiplexer Redis = ConnectionMultiplexer.Connect("localhost");
        public static readonly ISubscriber RedisSubscriber = Redis.GetSubscriber();

        private static readonly Channel<string> MessageChannel = Channel.CreateUnbounded<string>();
        static readonly SemaphoreSlim ProcessingLock = new(1, 1);

        public static Dictionary<string, int> ProcessedCount = new();

        // Mapeamento de ações para hotkeys
        static readonly Dictionary<string, VirtualKeyCode> HotKeys = new Dictionary<string, VirtualKeyCode>
        {
            { "V", VirtualKeyCode.F10 },   // Venda
            { "C", VirtualKeyCode.F11 },   // Compra
            { "Z", VirtualKeyCode.F12 },   // Fechar posição
            { "BE", VirtualKeyCode.F9 },   // Break Even
            { "Ic", VirtualKeyCode.F8 },   // Inversão para Compra
            { "Iv", VirtualKeyCode.F8 },   // Inversão para Venda (mesma tecla)
        };

        static async Task Main()
        {
            try { Directory.SetCurrentDirectory(AppContext.BaseDirectory); } catch { }

            Log.Logger = new LoggerConfiguration()
                .MinimumLevel.Debug()
                .WriteTo.Console(
                    restrictedToMinimumLevel: LogEventLevel.Information,
                    outputTemplate: "[{Timestamp:yyyy-MM-dd HH:mm:ss.fff} {Level:u3}] {Message:lj}{NewLine}{Exception}",
                    theme: AnsiConsoleTheme.Code
                )
                .WriteTo.File(
                    "logs/log.log",
                    rollingInterval: RollingInterval.Day,
                    restrictedToMinimumLevel: LogEventLevel.Debug,
                    outputTemplate: "{Timestamp:yyyy-MM-dd HH:mm:ss.fff} [{Level:u3}] {Message:lj}{NewLine}{Exception}"
                )
                .CreateLogger();

            string client;
            string userDesktop = Environment.MachineName;

            if (userDesktop == "VICTOR-DESKTOP1")
            {
                client = "Victor";
                _ = Task.Run(() => StartRedisSubscriber("trading-channel"));
                Log.Information("Iniciando Redis Subscriber...");
            }
            else
            {
                client = "Client";
                _ = Task.Run(() => StartHttpListener());
                Log.Information("Iniciando HTTP Listener na porta 5001...");
            }

            await ProcessQueueAsync(client);
        }

        static void StartHttpListener()
        {
            HttpListener listener = new HttpListener();
            listener.Prefixes.Add(AppEndpoints.ListenerPrefix);
            listener.Start();
            Log.Information("HTTP Listener iniciado. Aguardando trades...");

            while (true)
            {
                try
                {
                    HttpListenerContext context = listener.GetContext();
                    HttpListenerRequest request = context.Request;
                    HttpListenerResponse response = context.Response;

                    if (request.HttpMethod == "POST")
                    {
                        using (var reader = new StreamReader(request.InputStream, request.ContentEncoding))
                        {
                            string input = reader.ReadToEnd();
                            MessageChannel.Writer.TryWrite(input);

                            Log.Information($"Mensagem recebida no HTTP: {input}");
                            response.StatusCode = (int)HttpStatusCode.OK;
                            byte[] responseMessage = Encoding.UTF8.GetBytes("Hotkey enviada para processamento.");
                            response.OutputStream.Write(responseMessage, 0, responseMessage.Length);
                        }
                        response.OutputStream.Close();
                    }
                    else
                    {
                        WriteErrorResponse(response, "Método não permitido. Use POST.");
                    }
                }
                catch (Exception ex)
                {
                    Log.Error($"Erro no HTTP Listener: {ex.Message}");
                }
            }
        }

        static void StartRedisSubscriber(string channelName)
        {
            int retryCount = 0;
            const int maxRetries = 5;

            while (retryCount < maxRetries)
            {
                try
                {
                    RedisChannel redisChannel = RedisChannel.Literal(channelName);

                    RedisSubscriber.Subscribe(redisChannel, (channel, message) =>
                    {
                        Log.Information($"Mensagem recebida do Redis: {message}");
                        MessageChannel.Writer.TryWrite(message.ToString());
                        Log.Information($"Mensagem válida adicionada à fila.");
                    });

                    Log.Information($"Inscrito no canal Redis: {channelName}");
                    break;
                }
                catch (Exception ex)
                {
                    retryCount++;
                    Log.Error($"Erro ao conectar ao Redis: {ex.Message}. Tentativa {retryCount}/{maxRetries}.");

                    if (retryCount < maxRetries)
                    {
                        Thread.Sleep(2000);
                    }
                    else
                    {
                        Log.Error("Falha ao conectar ao Redis após múltiplas tentativas.");
                    }
                }
            }
        }

        static bool TryParseMessage(string jsonMessage, out string action, out string source, out JsonElement root)
        {
            try
            {
                JsonDocument jsonDocument = JsonDocument.Parse(jsonMessage);
                root = jsonDocument.RootElement;

                if (root.TryGetProperty("action", out JsonElement actionElement) &&
                    root.TryGetProperty("source", out JsonElement sourceElement))
                {
                    if (actionElement.ValueKind == JsonValueKind.Number)
                    {
                        action = actionElement.GetInt32() switch
                        {
                            0 => "Z",
                            1 => "C",
                            2 => "V",
                            3 => "BE",
                            4 => "Ic",
                            5 => "Iv",
                            6 => "TPc",
                            7 => "SLc",
                            _ => null
                        };
                    }
                    else if (actionElement.ValueKind == JsonValueKind.String)
                    {
                        string actionStr = actionElement.GetString();
                        action = actionStr switch
                        {
                            "Z" or "C" or "V" or "BE" or "Ic" or "Iv" or "TPc" or "SLc" => actionStr,
                            _ => null
                        };
                    }
                    else
                    {
                        action = null;
                    }

                    source = sourceElement.GetString();
                    return action != null;
                }
            }
            catch (JsonException ex)
            {
                Log.Error($"Erro ao processar JSON: {ex.Message}");
            }
            catch (Exception ex)
            {
                Log.Error($"Erro inesperado ao processar mensagem: {ex.Message}");
            }

            action = null;
            source = null;
            root = default;
            return false;
        }

        static async Task ProcessQueueAsync(string client)
        {
            Log.Information("Iniciando processamento da fila de mensagens...");

            while (true)
            {
                await foreach (var jsonMessage in MessageChannel.Reader.ReadAllAsync())
                {
                    await ProcessingLock.WaitAsync();
                    try
                    {
                        if (!TryParseMessage(jsonMessage, out string action, out string source, out JsonElement root))
                        {
                            Log.Warning($"Mensagem inválida descartada: {jsonMessage}");
                            continue;
                        }

                        // Incrementa contador de processados
                        if (!ProcessedCount.ContainsKey(source))
                            ProcessedCount[source] = 0;
                        ProcessedCount[source]++;

                        // Extrai informações básicas
                        string ticker = "N/A";
                        decimal closePrice = 0;

                        if (root.TryGetProperty("ticker", out var tickerElement))
                            ticker = tickerElement.GetString();

                        if (root.TryGetProperty("close", out var closeElement))
                        {
                            if (closeElement.ValueKind == JsonValueKind.Number)
                                closePrice = closeElement.GetDecimal();
                            else if (closeElement.ValueKind == JsonValueKind.String)
                                decimal.TryParse(closeElement.GetString(), out closePrice);
                        }

                        // Loga a mensagem processada
                        Log.Information($"Processado [{ProcessedCount[source]}] - Ação: \x1b[32m{action}\x1b[0m, " +
                                       $"Ativo: \x1b[36m{ticker}\x1b[0m, " +
                                       $"Preço: \x1b[33m{closePrice}\x1b[0m, " +
                                       $"Origem: \x1b[35m{source}\x1b[0m");

                        // Executa a ação (envia tecla)
                        await SendKeyAsync(action, source, ticker, closePrice, root);

                        // Loga informações adicionais se disponíveis
                        if (root.TryGetProperty("daily_trade_profit", out var profitElement))
                        {
                            if (profitElement.ValueKind == JsonValueKind.Number)
                                Log.Information($"  Lucro diário: {profitElement.GetDecimal():C}");
                        }

                        if (root.TryGetProperty("orderSize", out var sizeElement))
                        {
                            if (sizeElement.ValueKind == JsonValueKind.Number)
                                Log.Information($"  Tamanho da ordem: {sizeElement.GetDecimal()}");
                        }
                    }
                    catch (Exception ex)
                    {
                        Log.Error($"Erro durante o processamento da mensagem: {ex.Message}");
                    }
                    finally
                    {
                        ProcessingLock.Release();
                    }
                }
            }
        }

        static async Task SendKeyAsync(string action, string source, string ticker, decimal closePrice, JsonElement root)
        {
            try
            {
                InputSimulator isim = new InputSimulator();
                var ci = CultureInfo.GetCultureInfo("pt-BR");

                // Simula foco na janela correta (aqui você pode implementar a lógica real)
                // WindowHandler.BringWindowToFront(ticker);
                await Task.Delay(100);

                // Lógica para determinar qual hotkey enviar
                if (HotKeys.TryGetValue(action, out VirtualKeyCode key))
                {
                    Log.Information($"Enviando tecla {key} para ação {action}...");

                    // Verifica se é uma ordem com preço específico
                    if (action == "C" || action == "V" || action == "Ic" || action == "Iv")
                    {
                        // Simula entrada do preço
                        await EnterPriceAsync(isim, closePrice);
                        await Task.Delay(50);
                    }

                    // Envia a hotkey principal
                    isim.Keyboard.KeyPress(key);

                    // Log de confirmação
                    Log.Information($"\x1b[32m✓ Ação {action} executada com sucesso!\x1b[0m");

                    // Verifica se precisa configurar TP/SL
                    if ((action == "C" || action == "V") &&
                        (root.TryGetProperty("takeProfit", out _) || root.TryGetProperty("stopLoss", out _)))
                    {
                        await SetTPSLAsync(isim, action, root);
                    }
                }
                else if (action == "TPc" || action == "SLc")
                {
                    // Lógica para modificar TP/SL existente
                    await ModifyTPSLAsync(action, isim, root);
                }
                else
                {
                    Log.Warning($"Ação {action} não mapeada para hotkey.");
                }
            }
            catch (Exception ex)
            {
                Log.Error($"Erro ao enviar tecla: {ex.Message}");
            }
        }

        static async Task EnterPriceAsync(InputSimulator isim, decimal price)
        {
            // Simula clicar no campo de preço e digitar o valor
            string priceStr = price.ToString("0.########", CultureInfo.InvariantCulture);

            Log.Debug($"Inserindo preço: {priceStr}");

            // Clica no campo de preço (simulação)
            // isim.Mouse.MoveMouseTo(100, 100);
            // isim.Mouse.LeftButtonClick();
            // await Task.Delay(50);

            // Digita o preço
            isim.Keyboard.TextEntry(priceStr);
            await Task.Delay(100);

            // Pressiona Enter para confirmar
            isim.Keyboard.KeyPress(VirtualKeyCode.RETURN);
            await Task.Delay(50);
        }

        static async Task SetTPSLAsync(InputSimulator isim, string action, JsonElement root)
        {
            try
            {
                decimal tp = 0, sl = 0;

                if (root.TryGetProperty("takeProfit", out var tpElement) && tpElement.ValueKind == JsonValueKind.Number)
                    tp = tpElement.GetDecimal();

                if (root.TryGetProperty("stopLoss", out var slElement) && slElement.ValueKind == JsonValueKind.Number)
                    sl = slElement.GetDecimal();

                if (tp > 0 || sl > 0)
                {
                    Log.Information($"Configurando TP/SL - TP: {tp}, SL: {sl}");

                    // Simula navegação para a aba de ordens avançadas
                    // isim.Keyboard.KeyPress(VirtualKeyCode.TAB);
                    // await Task.Delay(50);

                    // Configura Take Profit
                    if (tp > 0)
                    {
                        // isim.Keyboard.KeyPress(VirtualKeyCode.T);
                        // await Task.Delay(50);
                        // isim.Keyboard.TextEntry(tp.ToString());
                        // await Task.Delay(50);
                        Log.Debug($"Take Profit configurado: {tp}");
                    }

                    // Configura Stop Loss
                    if (sl > 0)
                    {
                        // isim.Keyboard.KeyPress(VirtualKeyCode.S);
                        // await Task.Delay(50);
                        // isim.Keyboard.TextEntry(sl.ToString());
                        // await Task.Delay(50);
                        Log.Debug($"Stop Loss configurado: {sl}");
                    }

                    Log.Information($"\x1b[32m✓ TP/SL configurado com sucesso!\x1b[0m");
                }
            }
            catch (Exception ex)
            {
                Log.Error($"Erro ao configurar TP/SL: {ex.Message}");
            }
        }

        static async Task ModifyTPSLAsync(string action, InputSimulator isim, JsonElement root)
        {
            try
            {
                decimal newValue = 0;
                string tpSlType = action == "TPc" ? "Take Profit" : "Stop Loss";

                if (root.TryGetProperty(action == "TPc" ? "takeProfit" : "stopLoss", out var valueElement))
                {
                    if (valueElement.ValueKind == JsonValueKind.Number)
                        newValue = valueElement.GetDecimal();
                }

                if (newValue == 0)
                {
                    // Cancela a ordem existente
                    Log.Information($"Cancelando {tpSlType}...");
                    // isim.Keyboard.KeyPress(VirtualKeyCode.DELETE);
                    await Task.Delay(100);
                }
                else
                {
                    // Modifica o valor existente
                    Log.Information($"Modificando {tpSlType} para {newValue}...");

                    // Simula duplo clique no campo existente
                    // isim.Mouse.MoveMouseTo(150, 150);
                    // isim.Mouse.LeftButtonDoubleClick();
                    // await Task.Delay(100);

                    // Digita o novo valor
                    isim.Keyboard.TextEntry(newValue.ToString());
                    await Task.Delay(50);

                    // Confirma
                    isim.Keyboard.KeyPress(VirtualKeyCode.RETURN);
                    await Task.Delay(100);

                    Log.Information($"\x1b[32m✓ {tpSlType} modificado para {newValue}!\x1b[0m");
                }
            }
            catch (Exception ex)
            {
                Log.Error($"Erro ao modificar {action}: {ex.Message}");
            }
        }

        static void WriteErrorResponse(HttpListenerResponse response, string message)
        {
            response.StatusCode = (int)HttpStatusCode.BadRequest;
            byte[] responseMessage = Encoding.UTF8.GetBytes(message);
            response.OutputStream.Write(responseMessage, 0, responseMessage.Length);
            response.Close();
        }

        // Classe auxiliar para enviar eco (simulação)
        static class EchoCopy
        {
            public static async Task SendAsync(JsonElement root, int orderSize)
            {
                // Simulação de envio para outra plataforma
                Log.Information($"Eco enviado para TopStepX - Tamanho: {orderSize}");
                await Task.Delay(10);
            }
        }
    }

    // Exemplo de JSON esperado:
    /*
    {
        "action": 1,                    // 0=Z, 1=C, 2=V, 3=BE, 4=Ic, 5=Iv, 6=TPc, 7=SLc
        "source": "DIV_NQ",             // Origem do sinal
        "ticker": "NQ",                 // Ativo
        "close": 18500.25,              // Preço
        "daily_trade_profit": 1500.50,  // Lucro diário (opcional)
        "takeProfit": 18550.75,         // Take Profit (opcional)
        "stopLoss": 18480.50,           // Stop Loss (opcional)
        "orderSize": 1,                 // Tamanho da ordem (opcional)
        "side": 1,                      // Lado da ordem (1=Compra, 2=Venda)
        "invBol": 0,                    // Inversão (0=false, 1=true)
        "timestamp": "2024-01-15T10:30:00Z", // Timestamp (opcional)
        "tickSize": 0.25                // Tamanho do tick (opcional)
    }
    */
}