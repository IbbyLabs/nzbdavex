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
        context.Response.OnCompleted(() =>
        {
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
        var max = Math.Max(1, configManager.GetMaxDownloadConnections());
        return new PrioritizedSemaphore(max, max, configManager.GetStreamingPriority());
    }
}
