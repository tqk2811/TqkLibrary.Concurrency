using Nito.AsyncEx;
using System;
using System.Threading;
using System.Threading.Tasks;

namespace TqkLibrary.Concurrency.WaitQueues
{
    /// <summary>
    /// 
    /// </summary>
    public class WaitQueue
    {
        readonly AsyncLock asyncLock = new AsyncLock();

        int _currentAccess = 0;

        int _MaxAccess = 2;
        /// <summary>
        /// 
        /// </summary>
        public int MaxAccess
        {
            get { return _MaxAccess; }
            set { _MaxAccess = value; AccessChanged?.Invoke(); }
        }
        event Action? AccessChanged;
        /// <summary>
        /// 
        /// </summary>
        public WaitQueue()
        {
        }
        private void _Increment()
        {
            Interlocked.Increment(ref _currentAccess);
        }
        private void _Decrement()
        {
            Interlocked.Decrement(ref _currentAccess);
            AccessChanged?.Invoke();
        }

        /// <summary>
        /// 
        /// </summary>
        /// <param name="cancellationToken"></param>
        /// <returns></returns>
        public async Task<IWaitHandle> WaitLockAsync(CancellationToken cancellationToken = default)
        {
            using (var l = await asyncLock.LockAsync(cancellationToken).ConfigureAwait(false))
            {
                while (Volatile.Read(ref _currentAccess) >= _MaxAccess)
                {
                    TaskCompletionSource<bool> tcs = new TaskCompletionSource<bool>(TaskCreationOptions.RunContinuationsAsynchronously);
                    using var register = cancellationToken.Register(() => tcs.TrySetCanceled());
                    Action action = () => tcs.TrySetResult(true);
                    try
                    {
                        AccessChanged += action;
                        if (Volatile.Read(ref _currentAccess) >= _MaxAccess)
                        {
                            await tcs.Task.ConfigureAwait(false);
                        }
                        else
                        {
                            break;
                        }
                    }
                    finally
                    {
                        AccessChanged -= action;
                    }

                }
                return new WaitHandle(this);
            }
        }

        class WaitHandle : IWaitHandle
        {
            readonly WaitQueue waitQueue;
            int _isDisposed = 0;
            public WaitHandle(WaitQueue waitQueue)
            {
                this.waitQueue = waitQueue ?? throw new ArgumentNullException(nameof(waitQueue));
                this.waitQueue._Increment();
            }
            public void Dispose()
            {
                //guard double-dispose: otherwise _currentAccess is decremented twice
                //and more callers than MaxAccess can enter
                if (Interlocked.Exchange(ref _isDisposed, 1) == 0)
                    waitQueue._Decrement();
            }
        }
    }
}
