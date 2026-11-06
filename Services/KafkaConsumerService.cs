using Confluent.Kafka;
using System.Text.Json;
using KafkaDemo.Contracts;

namespace KafkaDemo.Services;

public class KafkaConsumerService : BackgroundService
{
 private readonly IConfiguration _configuration;
    private readonly ILogger<KafkaConsumerService> _logger;

    public KafkaConsumerService(
        IConfiguration configuration,
        ILogger<KafkaConsumerService> logger)
    {
        _configuration = configuration;
        _logger = logger;
    }

    protected override Task ExecuteAsync(
        CancellationToken stoppingToken)
    {
        return Task.Run(() =>
        {
            var config = new ConsumerConfig
            {
                BootstrapServers =
                    _configuration["Kafka:BootstrapServers"],

                GroupId = _configuration["Kafka:GroupId"],

                AutoOffsetReset = AutoOffsetReset.Earliest,

                EnableAutoCommit = false
            };

            using var consumer =
                new ConsumerBuilder<Ignore, string>(config).Build();

            consumer.Subscribe(_configuration["Kafka:Topic"]);

            try
            {
                while (!stoppingToken.IsCancellationRequested)
                {
                    try
                    {
                        var result = consumer.Consume(stoppingToken);

                        var order =
                            JsonSerializer.Deserialize<OrderCreatedEvent>(
                                result.Message.Value);

                        if (order != null)
                        {
                            _logger.LogInformation(
                                "Order diterima: {OrderId}, Customer: {Customer}",
                                order.OrderId,
                                order.CustomerName);

                            // Proses bisnis di sini.
                        }

                        consumer.Commit(result);
                    }
                    catch (ConsumeException ex)
                    {
                        _logger.LogError(
                            ex, "Kafka consume error");
                    }
                }
            }
            catch (OperationCanceledException)
            {
                _logger.LogInformation("Kafka consumer dihentikan.");
            }
            finally
            {
                consumer.Close();
            }
        }, CancellationToken.None);
    }
}
