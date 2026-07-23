using System.Threading;
using System.Threading.Tasks;

namespace TqkLibrary.Concurrency.TaskQueues
{
    /// <summary>
    ///
    /// </summary>
    public abstract class BaseWork : IWork
    {
        readonly CancellationTokenSource _cancellationTokenSource = new CancellationTokenSource();
        int _isDisposed = 0;

        /// <summary>
        ///
        /// </summary>
        protected CancellationToken CancellationToken { get { return _cancellationTokenSource.Token; } }

        /// <summary>
        ///
        /// </summary>
        public virtual bool IsPrioritize { get; set; } = false;

        /// <summary>
        /// Safe to call after <see cref="Dispose"/> (no-op instead of <see cref="System.ObjectDisposedException"/>,
        /// because WorkQueue may cancel and dispose concurrently)
        /// </summary>
        public virtual void Cancel()
        {
            if (Volatile.Read(ref _isDisposed) != 0) return;
            try
            {
                _cancellationTokenSource.Cancel();
            }
            catch (System.ObjectDisposedException)
            {
                //disposed between the check above and Cancel -> ignore
            }
        }

        /// <summary>
        /// Idempotent (safe to call multiple times)
        /// </summary>
        public virtual void Dispose()
        {
            if (Interlocked.Exchange(ref _isDisposed, 1) == 0)
            {
                _cancellationTokenSource.Dispose();
            }
        }

        /// <summary>
        ///
        /// </summary>
        /// <returns></returns>
        public abstract Task DoWorkAsync();
    }
}
