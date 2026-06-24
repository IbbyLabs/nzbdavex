using Microsoft.AspNetCore.Http;
using NzbWebDAV.Clients.Usenet.Concurrency;
using NzbWebDAV.Clients.Usenet.Contexts;
using NzbWebDAV.Config;
using NzbWebDAV.Extensions;

namespace NzbWebDAV.WebDav.Base;

public abstract class BaseStoreStreamFile(HttpContext context, ConfigManager configManager) : BaseStoreReadonlyItem
{
    protected abstract Task<Stream> GetStreamAsync(CancellationToken cancellationToken);

    public override Task<Stream> GetReadableStreamAsync(CancellationToken cancellationToken)
    {
        var streamSemaphore = CreatePerStreamSemaphore();
        var downloadPriorityContext = new DownloadPriorityContext()
        {
            Priority = SemaphorePriority.High,
            StreamSemaphore = streamSemaphore,
        };
        var scopedDownloadPriorityContext = cancellationToken.SetContext(downloadPriorityContext);

        // Keep this stream's per-stream budget in sync with live config changes,
        // mirroring how DownloadingNntpClient resizes the shared streaming
        // semaphore. The per-stream count depends on the total connection
        // setting, the per-stream preset, and (in auto mode) the provider pool.
        //
        // The per-stream enable toggle (usenet.max-download-connections-per-stream)
        // is intentionally excluded: whether a stream gets its own budget is decided
        // once in CreatePerStreamSemaphore when the stream starts, so flipping the
        // switch only affects newly created streams — an in-flight stream keeps the
        // mode it began with.
        EventHandler<ConfigManager.ConfigEventArgs>? onConfigChanged = null;
        if (streamSemaphore is { } perStreamSemaphore)
        {
            onConfigChanged = (_, e) =>
            {
                if (e.ChangedConfig.ContainsKey("usenet.max-download-connections")
                    || e.ChangedConfig.ContainsKey("usenet.max-download-connections-per-stream-preset")
                    || e.ChangedConfig.ContainsKey("usenet.providers"))
                {
                    perStreamSemaphore.UpdateMaxAllowed(configManager.GetMaxDownloadConnectionsPerStreamCount());
                }
            };
            configManager.OnConfigChanged += onConfigChanged;
        }

        context.Response.OnCompleted(() =>
        {
            if (onConfigChanged is not null) configManager.OnConfigChanged -= onConfigChanged;
            scopedDownloadPriorityContext.Dispose();
            streamSemaphore?.Dispose();
            return Task.CompletedTask;
        });

        return GetStreamAsync(cancellationToken);
    }

    // In "per stream" mode each playback session gets its own streaming semaphore
    // so concurrent streams don't share a single global budget. Returns null when
    // the mode is disabled — the shared global semaphore in DownloadingNntpClient
    // is used instead. The provider connection pool still caps real connections.
    private PrioritizedSemaphore? CreatePerStreamSemaphore()
    {
        if (!configManager.IsMaxDownloadConnectionsPerStream()) return null;
        var max = configManager.GetMaxDownloadConnectionsPerStreamCount();
        return new PrioritizedSemaphore(max, max, configManager.GetStreamingPriority());
    }
}
