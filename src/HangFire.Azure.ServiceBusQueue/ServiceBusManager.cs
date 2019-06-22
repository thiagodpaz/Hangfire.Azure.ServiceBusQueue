using System;
using System.Collections.Generic;
using Microsoft.Azure.ServiceBus;
using Microsoft.Azure.ServiceBus.Management;
using Hangfire.Logging;
using System.Threading.Tasks;
using Microsoft.Azure.ServiceBus.Core;

namespace Hangfire.Azure.ServiceBusQueue
{
    internal class ServiceBusManager
    {
        private static readonly ILog Logger = LogProvider.GetCurrentClassLogger();

        // Stores the pre-created QueueClients (note the key is the unprefixed queue name)
        private readonly Dictionary<string, (QueueClient, MessageReceiver)> _clients;
        private readonly ManagementClient _managementClient;

        public ServiceBusManager(ServiceBusQueueOptions options)
        {
            if (options == null) throw new ArgumentNullException("options");

            Options = options;

            _clients = new Dictionary<string, (QueueClient, MessageReceiver)>(options.Queues.Length);
            _managementClient = new ManagementClient(options.ConnectionString);
            if (options.CheckAndCreateQueues)
            {
                Task.Run(() => CreateQueueClients()).Wait();
            }
        }

        ~ServiceBusManager()
        {
            foreach (var keyValue in _clients)
            {
                var (queueClient, messageReceiver) = keyValue.Value;
                Task.Run(async () =>
                {
                    await queueClient.CloseAsync();
                    await messageReceiver.CloseAsync();
                }).Wait();
            }
        }

        public ServiceBusQueueOptions Options { get; }

        public async Task<(QueueClient, MessageReceiver)> GetClientAsync(string queue)
        {
            if (_clients.Count != Options.Queues.Length)
            {
                await CreateQueueClients().ConfigureAwait(false);
            }

            return _clients[queue];
        }

        public Task<QueueDescription> GetDescriptionAsync(string queue)
        {
            return _managementClient.GetQueueAsync(Options.GetQueueName(queue));
        }

        public Task<QueueRuntimeInfo> GetQueueRuntimeInfoAsync(string queue)
        {
            return _managementClient.GetQueueRuntimeInfoAsync(Options.GetQueueName(queue));
        }

        private async Task CreateQueueClients()
        {
            foreach (var queue in Options.Queues)
            {
                var prefixedQueue = Options.GetQueueName(queue);

                await CreateQueueIfNotExistsAsync(prefixedQueue).ConfigureAwait(false);

                Logger.TraceFormat("Creating new QueueClient for queue {0}", prefixedQueue);

                // Do not store as prefixed queue to avoid having to re-create name in GetClient method
                if (!_clients.ContainsKey(queue))
                {
                    _clients[queue] = (
                        new QueueClient(Options.ConnectionString, prefixedQueue, ReceiveMode.PeekLock),
                        new MessageReceiver(Options.ConnectionString, prefixedQueue, ReceiveMode.PeekLock)
                        );
                }
            }
        }

        private async Task CreateQueueIfNotExistsAsync(string prefixedQueue)
        {
            if (Options.CheckAndCreateQueues == false)
            {
                Logger.InfoFormat("Not checking for the existence of the queue {0}", prefixedQueue);

                return;
            }

            try
            {
                Logger.InfoFormat("Checking if queue {0} exists", prefixedQueue);

                if (await _managementClient.QueueExistsAsync(prefixedQueue).ConfigureAwait(false))
                {
                    return;
                }

                Logger.InfoFormat("Creating new queue {0}", prefixedQueue);

                var description = new QueueDescription(prefixedQueue);
                if (Options.RequiresDuplicateDetection != null)
                {
                    description.RequiresDuplicateDetection = Options.RequiresDuplicateDetection.Value;
                }

                if (Options.Configure != null)
                {
                    Options.Configure(description);
                }

                await _managementClient.CreateQueueAsync(description).ConfigureAwait(false);
            }
            catch (UnauthorizedAccessException ex)
            {
                var errorMessage = string.Format(
                    "Queue '{0}' could not be checked / created, likely due to missing the 'Manage' permission. " +
                    "You must either grant the 'Manage' permission, or set ServiceBusQueueOptions.CheckAndCreateQueues to false",
                    prefixedQueue);

                throw new UnauthorizedAccessException(errorMessage, ex);
            }
        }
    }
}
