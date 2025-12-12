using System.IO;
using System.Reflection;
using System.Runtime.Versioning;
using Microsoft.Playwright;
using Serilog;
using Serilog.Events;
using Serilog.Sinks.SystemConsole.Themes;

[assembly: SupportedOSPlatform("windows")]

namespace CopyReader
{
    public static class OrderReader
    {
        private static void ExtractEmbeddedResources(string targetDir)
        {
            var assembly = Assembly.GetExecutingAssembly();
            var resourceNames = assembly.GetManifestResourceNames();

            foreach (var resourceName in resourceNames)
            {
                if (!resourceName.StartsWith("pwdriver."))
                    continue;

                var fileName = resourceName.Substring("pwdriver.".Length);
                var filePath = Path.Combine(targetDir, fileName);

                Directory.CreateDirectory(Path.GetDirectoryName(filePath));

                using var stream = assembly.GetManifestResourceStream(resourceName);
                using var fileStream = File.Create(filePath);
                stream.CopyTo(fileStream);
            }
        }

        static async Task Main()
        {
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


            var baseDir = AppContext.BaseDirectory;
            try { Directory.SetCurrentDirectory(baseDir); } catch { }

            var driverDir = Path.Combine(baseDir, "pwdriver");
            Directory.CreateDirectory(driverDir);
            ExtractEmbeddedResources(driverDir);

            Environment.SetEnvironmentVariable("PLAYWRIGHT_BROWSERS_PATH", "0");
            Environment.SetEnvironmentVariable("PLAYWRIGHT_DRIVER_PATH", driverDir);
            Environment.SetEnvironmentVariable("PLAYWRIGHT_SKIP_VALIDATE_HOST_REQUIREMENTS", "true");

            _ = Microsoft.Playwright.Program.Main(new[] { "install", "chromium" });

            // TEMP MANUAL SWITCH - "socket" (paper trading nativo) ou "fetch" (logado pelo tradovate)
            var monitor = "socket";
            var targetUrl = "https://br.tradingview.com/chart/";

            if (monitor.Equals("socket", StringComparison.OrdinalIgnoreCase))
            {
                var sm = new SocketMonitor();
                await sm.Start(targetUrl);
                await Task.Delay(Timeout.InfiniteTimeSpan);
            }
            else // fetch
            {
                var fm = new FetchMonitor();
                using var cts = new CancellationTokenSource();
                Console.CancelKeyPress += (_, e) => { e.Cancel = true; cts.Cancel(); };
                await fm.StartAsync(targetUrl, cts.Token);
            }

        }
    }

}