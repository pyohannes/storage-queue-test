using System;
using System.Collections.Generic;
using System.Configuration;
using System.Diagnostics;
using System.Linq;
using System.Threading.Tasks;
using Azure.Storage.Queues;
using Azure.Storage.Queues.Models;

namespace Consumer
{
    class Program
    {
        static private string _storageConnectionString = "";
        static private string _storageQueueName = "testqueue";

        // Hard-coded amount of EventData objects in one message.
        private const int _batchSize = 6;

        static void Main(string[] args)
        {
            var queueClient = new QueueClient(_storageConnectionString, _storageQueueName);

            queueClient.CreateIfNotExists();

            var count = 0;
            var stopWatch = new Stopwatch();

            stopWatch.Start();

            do
            {
                QueueMessage[] retrievedMessage = queueClient.ReceiveMessages();

                count += retrievedMessage.Length;

                Task.WhenAll(
                    retrievedMessage.Select(msg => queueClient.DeleteMessageAsync(msg.MessageId, msg.PopReceipt)));

                if (count % 100 == 0)
                {
                    var totalEvents = count * _batchSize;
                    var timeElapsed = stopWatch.Elapsed.TotalSeconds;
                    var eventsPerSecond = totalEvents / timeElapsed;
                    Console.WriteLine($"Received {count} messages, {count * _batchSize} events total in {timeElapsed} seconds, {eventsPerSecond} events total/second");
                }
            } while (true);
        }
    }
}
