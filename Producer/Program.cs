using System;
using System.Collections.Generic;
using System.Configuration;
using System.Diagnostics;
using System.Threading.Tasks;
using Azure.Storage.Queues;
using Azure.Storage.Queues.Models;

namespace StorageQueueTest
{
    class Program
    {
        static private string _storageConnectionString = "";
        static private string _storageQueueName = "testqueue";

        // An EventHub EventData event has a size of about 6-10kB.
        // Let's assume the worst case here: packing 6 EventData events, each 10kB, in
        // one message;
        private const int _messageSize = 1024 * 10;
        private const int _batchSize = 6;

        static async Task Main(string[] args)
        {
            var queueClient = new QueueClient(_storageConnectionString, _storageQueueName);

            queueClient.CreateIfNotExists();

            // Random message text.
            var messageText = CreateRandomMessage(_messageSize * _batchSize);

            var count = 0;
            var stopWatch = new Stopwatch();

            stopWatch.Start();

            List<Task> tasks = new List<Task>();
            for (int i = 0; i < 8; i++)
            {
                tasks.Add(queueClient.SendMessageAsync(messageText));
            }

            do
            {

                Task t = Task.WhenAny(tasks);

                tasks.Remove(t);
                tasks.Add(queueClient.SendMessageAsync(messageText));

                count += 1;
                if (count % 100 == 0)
                {
                    var totalEvents = count * _batchSize;
                    var timeElapsed = stopWatch.Elapsed.TotalSeconds;
                    var eventsPerSecond = totalEvents / timeElapsed;
                    Console.WriteLine($"Sent {count} messages, {count * _batchSize} events total in {timeElapsed} seconds, {eventsPerSecond} events total/second");
                }
            } while (true);
        }

        static string CreateRandomMessage(int size)
        {
            string value = "";

            Random rnd = new Random(DateTime.Now.Millisecond);
            for (int x = 0; x < size; x++)
            {
                int index = rnd.Next(48, 122);
                value += index;
            }

            return value;
        }
    }
}
