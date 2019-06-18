using System.Configuration;
using System.IO;
using Hangfire.Azure.ServiceBusQueue;
using Microsoft.Extensions.Configuration;

namespace HangFire.Azure.ServiceBusQueue.Tests
{
    public class TestServiceBusQueueOptions : ServiceBusQueueOptions
    {
        public TestServiceBusQueueOptions()
        {
            var iconfiguration = new ConfigurationBuilder()
                 .SetBasePath(Directory.GetCurrentDirectory())
                 .AddJsonFile("appsettings.json").Build();

            CheckAndCreateQueues = true;
            ConnectionString = iconfiguration["BusConnectionString"];
            QueuePrefix = "hf-sb-tests-";
            Queues = new[] { "test1", "test2", "test3" };
        }
    }
}