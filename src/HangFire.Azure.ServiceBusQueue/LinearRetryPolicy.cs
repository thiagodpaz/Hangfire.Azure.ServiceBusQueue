using System;
using System.Threading;
using System.Threading.Tasks;

namespace Hangfire.Azure.ServiceBusQueue
{
    public class LinearRetryPolicy : IRetryPolicy
    {
        public LinearRetryPolicy(int retryCount, TimeSpan retryDelay)
        {
            this.RetryDelay = retryDelay;
            this.RetryCount = retryCount;
        }

        public TimeSpan RetryDelay { get; private set; }

        public int RetryCount { get; private set; }

        public async Task Execute(Func<Task> action)
        {
            for (var i = 0; i < this.RetryCount; i++)
            {
                try
                {
                    await action();

                    return;
                }
                catch (TimeoutException)
                {
                    if (i == this.RetryCount - 1)
                    {
                        throw;
                    }

                    Thread.Sleep(this.RetryDelay);
                }
            }
        }
    }
}