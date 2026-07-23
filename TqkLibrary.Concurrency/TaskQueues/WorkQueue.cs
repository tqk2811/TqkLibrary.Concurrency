using Nito.AsyncEx;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Threading;
using System.Threading.Tasks;

namespace TqkLibrary.Concurrency.TaskQueues
{

    /// <summary>
    ///
    /// </summary>
    /// <typeparam name="T"></typeparam>
    /// <param name="task"></param>
    /// <param name="workEventArgs"></param>

    public delegate Task WorkComplete<T>(Task task, WorkEventArgs<T> workEventArgs) where T : IWork;
    /// <summary>
    ///
    /// </summary>

    public delegate void RunComplete();


    /// <summary>
    ///
    /// </summary>
    public class WorkQueue
    {
        /// <summary>
        ///
        /// </summary>
        protected readonly Random _random
#if NET6_0_OR_GREATER
            = Random.Shared;
#else
            = new Random(DateTime.Now.GetHashCode());
#endif
        /// <summary>
        ///
        /// </summary>
#pragma warning disable CS0067//virtual event, only raised by derived WorkQueue<T> override
        public virtual event RunComplete? OnRunComplete;
#pragma warning restore CS0067

        /// <summary>
        /// Non FIFO run
        /// </summary>
        public bool RunRandom { get; set; } = false;

        /// <summary>
        ///
        /// </summary>
        public TaskScheduler TaskScheduler { get; set; } = TaskScheduler.Default;

        /// <summary>
        /// if true use <see cref="AsyncContext"/> (single thread for asynchronous), default true<br/>
        /// Warning: each running work will hold one dedicated thread (<see cref="TaskCreationOptions.LongRunning"/>)
        /// for its whole lifetime, even while awaiting. High <c>MaxRun</c> means many threads.
        /// </summary>
        public bool UseAsyncContext { get; set; } = true;
    }

    /// <summary>
    ///
    /// </summary>
    /// <typeparam name="T"></typeparam>
    public class WorkQueue<T> : WorkQueue
        where T : IWork
    {
        /// <summary>
        /// Single lock protects <see cref="_Queues"/>, <see cref="_QueueSet"/> and <see cref="_Runnings"/>,
        /// so every state transition (queue -> running -> removed) and every count check is atomic.
        /// </summary>
        private readonly object _lock = new object();
        private readonly List<T> _Queues = new List<T>();
        private readonly HashSet<T> _QueueSet = new HashSet<T>();//O(1) duplicate check for Add/AddRange
        private readonly List<T> _Runnings = new List<T>();

        /// <summary>
        /// true after <see cref="OnRunComplete"/> was fired for the current busy -> idle transition,
        /// reset to false when new work is added. Prevents duplicated/spurious fires.
        /// </summary>
        private bool _isCompleteNotified = true;

        /// <summary>
        ///
        /// </summary>
        public override event RunComplete? OnRunComplete;
        /// <summary>
        ///
        /// </summary>
        public event WorkComplete<T>? OnWorkComplete;
        /// <summary>
        /// Raised when an internal callback throws (<see cref="OnWorkComplete"/> handler, <see cref="OnRunComplete"/> handler,
        /// <see cref="IDisposable.Dispose"/> of a work, or the <c>SetRunLockObject</c> callback).
        /// Without this the exception would be swallowed (or crash the process in older versions).
        /// </summary>
        public event Action<Exception>? OnException;

        private int _MaxRun = 0;
        /// <summary>
        ///
        /// </summary>
        public int MaxRun
        {
            get { return _MaxRun; }
            set
            {
                bool increased = value > _MaxRun;
                _MaxRun = value;
                if (increased) RunNewWorkAsync();
            }
        }

        /// <summary>
        ///
        /// </summary>
        public bool IsRunning { get { lock (_lock) return _Runnings.Count != 0 || _Queues.Count != 0; } }

        /// <summary>
        /// Snapshot of current queued works
        /// </summary>
        public IReadOnlyList<T> Queues { get { lock (_lock) return _Queues.ToArray(); } }

        /// <summary>
        /// Snapshot of current running works
        /// </summary>
        public IReadOnlyList<T> Runnings { get { lock (_lock) return _Runnings.ToArray(); } }

        /// <summary>
        ///
        /// </summary>
        public int RunningCount
        {
            get { lock (_lock) return _Runnings.Count; }
        }

        /// <summary>
        ///
        /// </summary>
        public int QueueCount
        {
            get { lock (_lock) return _Queues.Count; }
        }


        private Task RunNewWorkAsync() => Task.Run(RunNewWork);
        private void RunNewWork()
        {
            List<T> toStart = new List<T>();
            bool isCompleted = false;
            try
            {
                lock (_lock)
                {
                    //Prioritize: start immediately when added, NOT limited by MaxRun (by design)
                    foreach (var work in _Queues.Where(x => x.IsPrioritize).ToList())
                    {
                        if (IsLockObjectBusy(work)) continue;
                        MoveToRunning(work, toStart);
                    }

                    //Normal: fill free slots up to MaxRun, skip items blocked by RunLockObject
                    //instead of stopping at the first blocked one (no head-of-line blocking, no busy-loop)
                    while (_Runnings.Count < _MaxRun)
                    {
                        if (!TryFindEligible(out T work)) break;
                        MoveToRunning(work, toStart);
                    }

                    if (_Queues.Count == 0 && _Runnings.Count == 0 && !_isCompleteNotified)
                    {
                        _isCompleteNotified = true;
                        isCompleted = true;
                    }
                }
            }
            catch (Exception ex)
            {
                OnException?.Invoke(ex);
            }
            finally
            {
                foreach (var work in toStart) StartWork(work);
                if (isCompleted)
                {
                    try { OnRunComplete?.Invoke(); }
                    catch (Exception ex) { OnException?.Invoke(ex); }
                }
            }
        }

        //must be called inside lock (_lock)
        private void MoveToRunning(T work, List<T> toStart)
        {
            _Queues.Remove(work);
            _QueueSet.Remove(work);
            _Runnings.Add(work);
            toStart.Add(work);
        }

        //must be called inside lock (_lock)
        private bool TryFindEligible(out T work)
        {
            IEnumerable<T> candidates = RunRandom ? _Queues.OrderBy(x => _random.Next()) : _Queues;
            foreach (var item in candidates)
            {
                if (!IsLockObjectBusy(item))
                {
                    work = item;
                    return true;
                }
            }
            work = default!;
            return false;
        }

        //must be called inside lock (_lock)
        private bool IsLockObjectBusy(T item)
        {
            var obj_lock = _RunLockObject.Invoke(item);
            if (obj_lock is null) return false;
            return _Runnings.Any(x => obj_lock.Equals(_RunLockObject.Invoke(x)));
        }

        private void StartWork(T work)
        {
            if (UseAsyncContext)
            {
                Task.Factory.StartNew(
                    () => AsyncContext.Run(() => RunWorkAsync(work)),
                    CancellationToken.None,
                    TaskCreationOptions.LongRunning,
                    this.TaskScheduler);
            }
            else
            {
                Task.Factory.StartNew(
                    () => RunWorkAsync(work),
                    CancellationToken.None,
                    TaskCreationOptions.None,
                    this.TaskScheduler);
            }
        }

        private async Task RunWorkAsync(T work)
        {
            Task task = Task.CompletedTask;
            try
            {
                task = work.DoWorkAsync();
                await task;
            }
            catch (Exception ex)
            {
                //DoWorkAsync threw synchronously (task never assigned) -> wrap into a faulted task
                if (task is null || (!task.IsFaulted && !task.IsCanceled)) task = Task.FromException(ex);
            }
            await WorkCompleted(task, work);
        }

        /// <summary>
        /// Never throws: handler/dispose exceptions go to <see cref="OnException"/>,
        /// and the finally block guarantees the work is removed from <see cref="_Runnings"/>
        /// so the queue can not get stuck.
        /// </summary>
        private async Task WorkCompleted(Task result, T work)
        {
            _ = result.Exception;//mark observed, avoid UnobservedTaskException

            var queueEventArg = new WorkEventArgs<T>(work);
            try
            {
                var handlers = OnWorkComplete;
                if (handlers is not null)
                {
                    //await every subscriber, not only the last one of the multicast delegate
                    foreach (WorkComplete<T> handler in handlers.GetInvocationList())
                    {
                        Task? task = handler.Invoke(result, queueEventArg);
                        if (task is not null) await task;
                    }
                }
            }
            catch (Exception ex)
            {
                OnException?.Invoke(ex);
            }
            finally
            {
                if (queueEventArg.ShouldDispose)
                {
                    try { work.Dispose(); }
                    catch (Exception ex) { OnException?.Invoke(ex); }
                }
                lock (_lock) _Runnings.Remove(work);
                _ = RunNewWorkAsync();//much run on threadpool
            }
        }

        /// <summary>
        ///
        /// </summary>
        /// <param name="work"></param>
        /// <exception cref="ArgumentNullException"></exception>
        public bool Add(T work)
        {
            if (work is null) throw new ArgumentNullException(nameof(work));
            lock (_lock)
            {
                if (!_QueueSet.Add(work)) return false;
                _Queues.Add(work);
                _isCompleteNotified = false;
            }
            RunNewWorkAsync();
            return true;
        }

        /// <summary>
        ///
        /// </summary>
        /// <returns>Items was not add</returns>
        /// <param name="works"></param>
        /// <exception cref="ArgumentNullException"></exception>
        public IReadOnlyList<T> AddRange(IEnumerable<T> works)
        {
            if (null == works) throw new ArgumentNullException(nameof(works));
            int addCount = 0;
            List<T> result = new List<T>();
            lock (_lock)
            {
                foreach (var work in works)
                {
                    if (_QueueSet.Add(work))
                    {
                        _Queues.Add(work);
                        addCount++;
                    }
                    else
                    {
                        result.Add(work);
                    }
                }
                if (addCount > 0) _isCompleteNotified = false;
            }
            if (addCount > 0) RunNewWorkAsync();
            return result;
        }

        /// <summary>
        /// If the work is still queued: remove and dispose it (queue owns the work).<br/>
        /// If the work is running: request cancel (it will be disposed on completion by <see cref="OnWorkComplete"/> flow).
        /// </summary>
        /// <param name="work"></param>
        /// <exception cref="ArgumentNullException"></exception>
        public bool Cancel(T work)
        {
            if (work is null) throw new ArgumentNullException(nameof(work));
            bool removed = false;
            try
            {
                lock (_lock)
                {
                    if (_Queues.Remove(work))
                    {
                        _QueueSet.Remove(work);
                        work.TaskDispose();
                        removed = true;
                        return true;
                    }
                    if (_Runnings.Contains(work))
                    {
                        work.TaskCancel();
                        return true;
                    }
                }
                return false;
            }
            finally
            {
                //queue may become empty -> let waiter of WaitForShutDownAsync wake up
                if (removed) RunNewWorkAsync();
            }
        }

        /// <summary>
        ///
        /// </summary>
        /// <param name="func"></param>
        /// <exception cref="ArgumentNullException"></exception>
        /// <returns>Items removed in <see cref="Queues"/></returns>
        public IReadOnlyList<T> Cancel(Func<T, bool> func)
        {
            if (null == func) throw new ArgumentNullException(nameof(func));
            List<T> removes = new List<T>();
            lock (_lock)
            {
                foreach (var q in _Queues.Where(func))
                {
                    q.TaskDispose();
                    removes.Add(q);
                }
                removes.ForEach(x => { _Queues.Remove(x); _QueueSet.Remove(x); });
                foreach (var q in _Runnings.Where(func))
                {
                    q.TaskCancel();
                }
            }
            //queue may become empty -> let waiter of WaitForShutDownAsync wake up
            if (removes.Count > 0) RunNewWorkAsync();
            return removes;
        }

        Func<T, object?> _RunLockObject = (t) => null;
        /// <summary>
        /// Example: object1 + IQueue1<br></br>
        /// object1 + IQueue2<br></br>
        /// object2 + IQueue3<br></br>
        /// MaxRun=3<br></br>
        /// Then IQueue2 will wait IQueue1 done
        /// </summary>
        /// <param name="func">if return object and equal then wait.<br></br>Warning: Don't access TaskQueue from here</param>
        /// <exception cref="ArgumentNullException"></exception>
        public void SetRunLockObject(Func<T, object> func)
        {
            if (func is null) throw new ArgumentNullException(nameof(func));
            _RunLockObject = func;
        }

        /// <summary>
        ///
        /// </summary>
        public void ShutDown(int maxRun = 0)
        {
            _MaxRun = 0;
            lock (_lock)
            {
                foreach (var queue in _Queues) queue.TaskDispose();
                _Queues.Clear();
                _QueueSet.Clear();
                foreach (var running in _Runnings) running.TaskCancel();
            }
            _MaxRun = maxRun;
            //queue may become empty -> let waiter of WaitForShutDownAsync wake up
            RunNewWorkAsync();
        }

        /// <summary>
        /// Note: blocking (sync over async), prefer <see cref="WaitForShutDownAsync"/>
        /// </summary>
        /// <param name="timeOut"></param>
        public bool WaitForShutDown(int timeOut = -1)
            => WaitForShutDownAsync(timeOut).ConfigureAwait(false).GetAwaiter().GetResult();

        /// <summary>
        ///
        /// </summary>
        /// <param name="timeOut"></param>
        /// <returns></returns>
        public async Task<bool> WaitForShutDownAsync(int timeOut = -1)
        {
            TaskCompletionSource<bool> taskCompletionSource = new TaskCompletionSource<bool>(TaskCreationOptions.RunContinuationsAsynchronously);
            RunComplete runComplete = () => taskCompletionSource.TrySetResult(true);
            //subscribe BEFORE checking state, so the complete event can not slip between check and subscribe
            this.OnRunComplete += runComplete;
            try
            {
                if (!IsRunning) return true;
                using CancellationTokenSource cancellationTokenSource = new CancellationTokenSource(timeOut);
                using var register = cancellationTokenSource.Token.Register(() => taskCompletionSource.TrySetResult(false));
                await taskCompletionSource.Task.ConfigureAwait(false);
                return !IsRunning;
            }
            finally
            {
                this.OnRunComplete -= runComplete;
            }
        }

    }
}
