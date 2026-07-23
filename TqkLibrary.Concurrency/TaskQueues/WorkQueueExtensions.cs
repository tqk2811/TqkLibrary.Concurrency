using System.Threading.Tasks;

namespace TqkLibrary.Concurrency.TaskQueues
{
    internal static class WorkQueueExtensions
    {
        //fire-and-forget on threadpool: swallow exceptions (ex: user IWork impl throws ObjectDisposedException)
        //to avoid UnobservedTaskException, since nobody awaits these tasks
        internal static Task TaskDispose(this IWork work)
        {
            if (work is not null) return Task.Run(() => { try { work.Dispose(); } catch { } });
            return Task.CompletedTask;
        }
        internal static Task TaskCancel(this IWork work)
        {
            if (work is not null) return Task.Run(() => { try { work.Cancel(); } catch { } });
            return Task.CompletedTask;
        }
    }
}