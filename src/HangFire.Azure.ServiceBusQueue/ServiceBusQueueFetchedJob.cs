using System;
using System.Diagnostics;
using System.Text;
using System.Threading;
using System.Threading.Tasks;
using Hangfire.Logging;
using Hangfire.Storage;
using Microsoft.Azure.ServiceBus;
using Microsoft.Azure.ServiceBus.Core;
using Newtonsoft.Json;

namespace Hangfire.Azure.ServiceBusQueue
{
    internal class ServiceBusQueueFetchedJob : IFetchedJob
    {
        private readonly ILog _logger = LogProvider.GetLogger(typeof(ServiceBusQueueFetchedJob));
        private readonly MessageReceiver _client;
        private readonly Message _message;
        private readonly TimeSpan? _lockRenewalDelay;
        private readonly CancellationTokenSource _cancellationTokenSource;

        private bool _completed;
        private bool _disposed;

        public ServiceBusQueueFetchedJob(MessageReceiver client, Message message, TimeSpan? lockRenewalDelay)
        {
            _client = client;
            _message = message ?? throw new ArgumentNullException("message");
            _lockRenewalDelay = lockRenewalDelay;
            _cancellationTokenSource = new CancellationTokenSource();

            JobId = Encoding.UTF8.GetString(message.Body);

            KeepAlive();
        }

        public string JobId { get; private set; }

        public Message Message => _message;

        public void Requeue()
        {
            _cancellationTokenSource.Cancel();

            Task.Run(() => _client.AbandonAsync(_message.SystemProperties.LockToken)).Wait();
            _completed = true;
        }

        public void RemoveFromQueue()
        {
            _cancellationTokenSource.Cancel();

            Task.Run(() => _client.CompleteAsync(_message.SystemProperties.LockToken)).Wait();
            _completed = true;
        }

        public void Dispose()
        {
            _cancellationTokenSource.Cancel();

            if (!_completed && !_disposed)
            {
                Task.Run(() => _client.AbandonAsync(_message.SystemProperties.LockToken)).Wait();
            }

            _disposed = true;
        }

        private void KeepAlive()
        {
            Task.Run(async () =>
            {
                while (!_cancellationTokenSource.Token.IsCancellationRequested)
                {
                    // Previously we were waiting until a second before the lock is due
                    // to expire to give us plenty of time to wake up and communicate
                    // with queue to renew the lock of this message before expiration.
                    // However since clocks may be non-synchronized well, for long-running
                    // background jobs it's better to have more renewal attempts than a
                    // lock that's expired too early.
                    var toWait = _lockRenewalDelay.HasValue
                        ? _lockRenewalDelay.Value
                        : _message.SystemProperties.LockedUntilUtc - DateTime.UtcNow - TimeSpan.FromSeconds(1);

                    await Task.Delay(toWait, _cancellationTokenSource.Token);

                    // Double check we have not been cancelled to avoid renewing a lock
                    // unnecessarily
                    if (!_cancellationTokenSource.Token.IsCancellationRequested)
                    {
                        try
                        {
                            await _client.RenewLockAsync(_message);
                        }
                        catch (Exception ex)
                        {
                            _logger.DebugException(
                                String.Format("An exception was thrown while trying to renew a lock for job '{0}'.", JobId),
                                ex);
                        }
                    }
                }
            }, _cancellationTokenSource.Token);
        }

        internal async Task DeadLetterAsync()
        {
            _cancellationTokenSource.Cancel();

            if (!_completed && !_disposed)
            {
                await _client.DeadLetterAsync(_message.SystemProperties.LockToken);
            }
        }
    }
}
