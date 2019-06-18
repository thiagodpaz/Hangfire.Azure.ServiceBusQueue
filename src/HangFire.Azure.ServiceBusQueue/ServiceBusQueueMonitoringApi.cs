using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using Hangfire.SqlServer;
using Microsoft.Azure.ServiceBus.Core;

namespace Hangfire.Azure.ServiceBusQueue
{
    internal class ServiceBusQueueMonitoringApi : IPersistentJobQueueMonitoringApi
    {
        private readonly ServiceBusManager _manager;
        private readonly string[] _queues;

        public ServiceBusQueueMonitoringApi(ServiceBusManager manager, string[] queues)
        {
            if (manager == null) throw new ArgumentNullException("manager");
            if (queues == null) throw new ArgumentNullException("queues");

            _manager = manager;
            _queues = queues;
        }

        public IEnumerable<string> GetQueues()
        {
            return _queues;
        }

        public IEnumerable<long> GetEnqueuedJobIds(string queue, int @from, int perPage)
        {
            var results = Task.Run(async () =>
            {
                var client = await _manager.GetClientAsync(queue);
                var messageReceiver = new MessageReceiver(client.ServiceBusConnection, client.Path, client.ReceiveMode);

                var jobIds = new List<long>();

                // We have to overfetch to retrieve enough messages for paging.
                // e.g. @from = 10 and page size = 20 we need 30 messages from the start
                var messages = await messageReceiver.PeekAsync(@from + perPage);

                // We could use LINQ here but to avoid creating lots of garbage lists
                // through .Skip / .ToList etc. use a simple loop.
                for (var i = 0; i < messages.Count; i++)
                {
                    var msg = messages[i];

                    // Only include the job id once we have skipped past the @from
                    // number
                    if (i >= @from)
                    {
                        var jobId = Encoding.UTF8.GetString(msg.Body);
                        if (long.TryParse(jobId, out var longJobId))
                        {
                            jobIds.Add(longJobId);
                        }
                    }
                }

                return jobIds;
            }).GetAwaiter().GetResult();
            return results;
        }

        public IEnumerable<long> GetFetchedJobIds(string queue, int @from, int perPage)
        {
            return Enumerable.Empty<long>();
        }

        public EnqueuedAndFetchedCountDto GetEnqueuedAndFetchedCount(string queue)
        {
            var result = Task.Run(async () =>
            {
                var queueRuntimeInfo = await _manager.GetQueueRuntimeInfoAsync(queue);

                return new EnqueuedAndFetchedCountDto
                {
                    EnqueuedCount = (int)queueRuntimeInfo.MessageCountDetails.ActiveMessageCount,
                    FetchedCount = null
                };
            }).GetAwaiter().GetResult();
            return result;
        }
    }
}
