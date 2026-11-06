using Confluent.Kafka;
using System.Text.Json;
using KafkaDemo.Contracts;

namespace KafkaDemo.Services;

public class KafkaProducerService : IDisposable
{
    private readonly IProducer<Null, string> _producer;
    private readonly IConfiguration _configuration;

    public KafkaProducerService(IConfiguration configuration)
    {
        _configuration = configuration;

        var config = new ProducerConfig
        {
            BootstrapServers = configuration["Kafka:BootstrapServers"],
            Acks = Acks.All,
            EnableIdempotence = true
        };

        _producer = new ProducerBuilder<Null, string>(config)
            .Build();
    }

    public async Task SendMessageAsync(
        OrderCreatedEvent order)
    {
        var topic = _configuration["Kafka:Topic"];

        var message = JsonSerializer.Serialize(order);

        await _producer.ProduceAsync(
            topic,
            new Message<Null, string>
            {
                Value = message
            });
    }

    public void Dispose()
    {
        _producer.Flush(TimeSpan.FromSeconds(10));
        _producer.Dispose();
    }
}