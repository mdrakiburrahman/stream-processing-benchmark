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
int senderCount = int.TryParse(Environment.GetEnvironmentVariable("SENDER_COUNT"), out var sc) && sc > 0 ? sc : coreCount * 8;
int clientCount = int.TryParse(Environment.GetEnvironmentVariable("CLIENT_COUNT"), out var cc) && cc > 0 ? cc : Math.Max(coreCount, 8);

Console.WriteLine($"Starting producer: {coreCount} cores, {senderCount} senders, {clientCount} AMQP connections targeting '{eventHubName}'");

using var cts = new CancellationTokenSource();
Console.CancelKeyPress += (_, e) => { e.Cancel = true; cts.Cancel(); };
AppDomain.CurrentDomain.ProcessExit += (_, _) => cts.Cancel();

// Pool of AMQP connections shared across senders
var clients = new EventHubProducerClient[clientCount];
for (int i = 0; i < clientCount; i++)
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
        Enumerable.Range(0, senderCount),
        new ParallelOptions { MaxDegreeOfParallelism = senderCount, CancellationToken = cts.Token },
        async (senderIndex, ct) =>
        {
            var client = clients[senderIndex % clientCount];
            long seq = 0;

            // Double-buffer: fill next batch while the current one is in flight
            Task? pendingSend = null;
            EventDataBatch? previousBatch = null;

            while (!ct.IsCancellationRequested)
            {
                EventDataBatch batch = await client.CreateBatchAsync(ct).ConfigureAwait(false);

                while (!ct.IsCancellationRequested)
                {
                    var payload = JsonSerializer.SerializeToUtf8Bytes(new
                    {
                        ts = DateTime.UtcNow.ToString("O"),
                        producer_id = senderIndex,
                        seq
                    });

                    if (!batch.TryAdd(new EventData(payload)))
                        break;

                    seq++;
                }

                if (batch.Count > 0)
                {
                    if (pendingSend != null)
                    {
                        await pendingSend.ConfigureAwait(false);
                        Interlocked.Add(ref totalSent, previousBatch!.Count);
                        previousBatch.Dispose();
                    }

                    previousBatch = batch;
                    pendingSend = client.SendAsync(batch, ct);
                }
                else
                {
                    batch.Dispose();
                }
            }

            if (pendingSend != null)
            {
                await pendingSend.ConfigureAwait(false);
                Interlocked.Add(ref totalSent, previousBatch!.Count);
                previousBatch!.Dispose();
            }
        });
}
catch (OperationCanceledException) { }

Console.WriteLine($"Shutting down. Total messages sent: {Interlocked.Read(ref totalSent):N0}");

// Dispose all clients
await Task.WhenAll(clients.Select(c => c.DisposeAsync().AsTask()));
Console.WriteLine("All clients disposed. Exiting.");
