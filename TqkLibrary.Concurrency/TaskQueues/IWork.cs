using System;
using System.Threading.Tasks;

namespace TqkLibrary.Concurrency.TaskQueues
{
    /// <summary>
    /// 
    /// </summary>
    public interface IWork : IDisposable
    {
        /// <summary>
        /// Prioritize: start immediately when added to the queue,
        /// NOT limited by <c>WorkQueue&lt;T&gt;.MaxRun</c> (by design)
        /// </summary>
        bool IsPrioritize { get; }

        /// <summary>
        /// Dont use <b>async void</b> inside<br/>
        /// </summary>
        /// <returns></returns>
        Task DoWorkAsync();

        /// <summary>
        /// 
        /// </summary>
        void Cancel();
    }
}