using KafkaDemo.Data;
using KafkaDemo.Middlewares;
using KafkaDemo.Services;
using KafkaDemo.Services.Interfaces;
using KafkaDemo.Services.Kafka;
using Microsoft.EntityFrameworkCore;
using Microsoft.Extensions.Options;
using Serilog;


var builder = WebApplication.CreateBuilder(args);
//log
builder.Host.UseSerilog((context, services, configuration) =>
{
    configuration
        .ReadFrom.Configuration(context.Configuration)
        .ReadFrom.Services(services)
        .Enrich.FromLogContext()
        .Enrich.WithProperty(
            "Application",
            context.HostingEnvironment.ApplicationName)
        .Enrich.WithProperty(
            "Environment",
            context.HostingEnvironment.EnvironmentName);
});

//kafka
var kafkaSection = builder.Configuration.GetSection("Kafka");
builder.Services.Configure<KafkaOptions>(kafkaSection);

builder.Services.AddSingleton(sp =>
    sp.GetRequiredService<IOptions<KafkaOptions>>().Value);

// Database
var connectionString =
    builder.Configuration.GetConnectionString("DefaultConnection")
    ?? throw new InvalidOperationException(
        "Database connection string is missing.");

builder.Services.AddDbContext<AppDbContext>(options =>
    options.UseNpgsql(connectionString));

// Kafka Producer
builder.Services.AddSingleton<IKafkaProducer,KafkaProducer>();
// Kafka Consumer
builder.Services.AddHostedService<KafkaConsumerWorker>();

// Business Services
builder.Services.AddScoped<IOrderService,OrderService>();
builder.Services.AddScoped<CorrelationContext>();
builder.Services.AddControllers();
builder.Services.AddEndpointsApiExplorer();
builder.Services.AddProblemDetails();


var app = builder.Build();
// Database initialization for local demonstration.
await using (var scope = app.Services.CreateAsyncScope())
{
    var db = scope.ServiceProvider
        .GetRequiredService<AppDbContext>();

    await db.Database.EnsureCreatedAsync();
}

// Configure the HTTP request pipeline.
if (app.Environment.IsDevelopment())
{
    app.MapOpenApi();
}

app.UseExceptionHandler();

app.UseMiddleware<CorrelationIdMiddleware>();

app.UseSerilogRequestLogging();
app.MapControllers();

app.MapGet("/health", () => Results.Ok(new
{
    Status = "Healthy",
    Application = "KafkaOrderApi",
    Time = DateTime.UtcNow
}));

app.Run();

