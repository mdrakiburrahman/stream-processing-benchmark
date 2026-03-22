using System;
using System.Collections.Concurrent;
using System.Diagnostics;
using System.Linq;
using System.Net;
using System.Text;
using System.Text.Json;
using System.Threading;
using System.Threading.Tasks;
using Azure.Messaging.EventHubs;
using Azure.Messaging.EventHubs.Producer;

string connectionString = Environment.GetEnvironmentVariable("EVENTHUB_CONNECTION_STRING")
    ?? throw new InvalidOperationException("EVENTHUB_CONNECTION_STRING is required");
string eventHubName = Environment.GetEnvironmentVariable("EVENTHUB_NAME") ?? "benchmark";

int coreCount = Environment.ProcessorCount;
Console.WriteLine($"Starting producer with {coreCount} cores targeting '{eventHubName}'");

using var cts = new CancellationTokenSource();
Console.CancelKeyPress += (_, e) => { e.Cancel = true; cts.Cancel(); };
AppDomain.CurrentDomain.ProcessExit += (_, _) => cts.Cancel();

// One producer client per core for separate AMQP connections
var clients = new EventHubProducerClient[coreCount];
for (int i = 0; i < coreCount; i++)
    clients[i] = new EventHubProducerClient(connectionString, eventHubName);

long totalSent = 0;
var sw = Stopwatch.StartNew();
long lastReportedCount = 0;
long lastReportedTicks = 0;

// Health probe — returns 200 once first batch sent
var healthListener = new HttpListener();
healthListener.Prefixes.Add("http://+:8080/health/");
healthListener.Start();
_ = Task.Run(async () =>
{
    while (!cts.Token.IsCancellationRequested)
    {
        try
        {
            var ctx = await healthListener.GetContextAsync().ConfigureAwait(false);
            bool ready = Interlocked.Read(ref totalSent) > 0;
            ctx.Response.StatusCode = ready ? 200 : 503;
            var body = Encoding.UTF8.GetBytes(ready ? "ok" : "warming up");
            await ctx.Response.OutputStream.WriteAsync(body, cts.Token).ConfigureAwait(false);
            ctx.Response.Close();
        }
        catch { break; }
    }
}, cts.Token);

// Stats reporter
_ = Task.Run(async () =>
{
    while (!cts.Token.IsCancellationRequested)
    {
        await Task.Delay(10_000, cts.Token).ConfigureAwait(false);
        long currentCount = Interlocked.Read(ref totalSent);
        long currentTicks = sw.ElapsedTicks;
        double intervalSec = (double)(currentTicks - lastReportedTicks) / Stopwatch.Frequency;
        long intervalMsgs = currentCount - lastReportedCount;
        double overallRate = currentCount / sw.Elapsed.TotalSeconds;

        Console.WriteLine(
            $"[Stats] Total sent: {currentCount:N0} | " +
            $"Last 10s: {intervalMsgs / intervalSec:N0} msg/s | " +
            $"Overall: {overallRate:N0} msg/s");

        lastReportedCount = currentCount;
        lastReportedTicks = currentTicks;
    }
}, cts.Token);

try
{
    await Parallel.ForEachAsync(
        Enumerable.Range(0, coreCount),
        new ParallelOptions { MaxDegreeOfParallelism = coreCount, CancellationToken = cts.Token },
        async (coreIndex, ct) =>
        {
            var client = clients[coreIndex];
            long seq = 0;

            while (!ct.IsCancellationRequested)
            {
                using EventDataBatch batch = await client.CreateBatchAsync(ct).ConfigureAwait(false);

                while (!ct.IsCancellationRequested)
                {
                    var payload = JsonSerializer.SerializeToUtf8Bytes(new
                    {
                        ts = DateTime.UtcNow.ToString("O"),
                        producer_id = coreIndex,
                        seq
                    });

                    var eventData = new EventData(payload);
                    if (!batch.TryAdd(eventData))
                        break;

                    seq++;
                }

                if (batch.Count > 0)
                {
                    await client.SendAsync(batch, ct).ConfigureAwait(false);
                    Interlocked.Add(ref totalSent, batch.Count);
                }
            }
        });
}
catch (OperationCanceledException) { }

Console.WriteLine($"Shutting down. Total messages sent: {Interlocked.Read(ref totalSent):N0}");

// Dispose all clients
await Task.WhenAll(clients.Select(c => c.DisposeAsync().AsTask()));
Console.WriteLine("All clients disposed. Exiting.");
