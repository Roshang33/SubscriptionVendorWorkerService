using Microsoft.Extensions.Hosting;
using Microsoft.Extensions.Logging;
using MongoDB.Bson;
using MongoDB.Driver;
using System;
using System.Threading;
using System.Threading.Tasks;

namespace SubscriptionVendorWorkerService
{
    public class MongoChangeStreamService : BackgroundService
    {
        private readonly ILogger<MongoChangeStreamService> _logger;
        private readonly IMongoClient _mongoClient;
        private readonly string _databaseName;
        private readonly string _collectionName;
        private readonly IMongoCollection<BsonDocument> _collection;

        public MongoChangeStreamService(ILogger<MongoChangeStreamService> logger)
        {
            _logger = logger;
            _databaseName = "subscription"; // Replace with your database name
            _collectionName = "subscriptionconfig"; // Replace with your collection name

            // Initialize the MongoDB client
            _mongoClient = new MongoClient("mongodb://localhost:27017/?replicaSet=rs0"); // Replace with your connection string
                                                                         // Get the collection
            var database = _mongoClient.GetDatabase(_databaseName);
            _collection = database.GetCollection<BsonDocument>(_collectionName);
        }

        protected override async Task ExecuteAsync(CancellationToken stoppingToken)
        {
            Console.WriteLine("Listening for MongoDB changes...");

            try
            {

                using var cursor = await _collection.WatchAsync(cancellationToken: stoppingToken);

                while (await cursor.MoveNextAsync(stoppingToken))
                {
                    foreach (var change in cursor.Current)
                    {
                        if (change != null)
                        {
                            switch (change.OperationType)
                            {
                                case ChangeStreamOperationType.Insert:
                                    Console.WriteLine("Insert detected:");
                                    Console.WriteLine(change.FullDocument);
                                    break;

                                case ChangeStreamOperationType.Update:
                                    Console.WriteLine("Update detected:");
                                    Console.WriteLine($"Updated Fields: {change.UpdateDescription?.UpdatedFields}");
                                    Console.WriteLine($"Removed Fields: {change.UpdateDescription?.RemovedFields}");
                                    break;

                                case ChangeStreamOperationType.Delete:
                                    Console.WriteLine("Delete detected:");
                                    Console.WriteLine($"Document Key: {change.DocumentKey}");
                                    break;

                                default:
                                    Console.WriteLine($"Other change type: {change.OperationType}");
                                    break;
                            }

                        }
                    }
                }


            }
            catch (Exception ex)
            {
                Console.WriteLine($"Error listening to changes: {ex.Message}");
            }
        }

        //private void ProcessChange(ChangeStreamDocument<BsonDocument> change)
        //{
        //    _logger.LogInformation("Change detected in MongoDB:");
        //    _logger.LogInformation("Operation Type: {OperationType}", change.OperationType);

        //    if (change.FullDocument != null)
        //    {
        //        _logger.LogInformation("Document: {Document}", change.FullDocument);
        //    }

        //    if (change.UpdateDescription != null)
        //    {
        //        _logger.LogInformation("Updated Fields: {UpdatedFields}", change.UpdateDescription.UpdatedFields);
        //        _logger.LogInformation("Removed Fields: {RemovedFields}", change.UpdateDescription.RemovedFields);
        //    }
        //}

        //public override Task StopAsync(CancellationToken cancellationToken)
        //{
        //    _logger.LogInformation("Stopping MongoDB change stream listener...");
        //    return base.StopAsync(cancellationToken);
        //}
    }
}
