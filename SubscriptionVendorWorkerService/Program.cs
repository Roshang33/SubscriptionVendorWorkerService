using SubscriptionVendorWorkerService;

var builder = Host.CreateApplicationBuilder(args);
builder.Services.AddHostedService<MongoChangeStreamService>();

var host = builder.Build();
host.Run();
