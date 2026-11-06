using System.Text;
using System.Text.Json;
using Confluent.Kafka;
using KafkaDemo.Services.Interfaces;

namespace KafkaDemo.Services.Kafka;

public sealed class KafkaProducer : IKafkaProducer, IDisposable
{
    private readonly IProducer<string, string> _producer;
    private readonly ILogger<KafkaProducer> _logger;

    private static readonly JsonSerializerOptions JsonOptions =
        new(JsonSerializerDefaults.Web);

    public KafkaProducer(
        KafkaOptions options,
        ILogger<KafkaProducer> logger)
    {
        _logger = logger;

        var config = new ProducerConfig
        {
            BootstrapServers = options.BootstrapServers,
            Acks = Acks.All,
            EnableIdempotence = true,
            MessageSendMaxRetries = 10,
            RetryBackoffMs = 500,
            MessageTimeoutMs = 30000
        };

        _producer = new ProducerBuilder<string, string>(config)
            .Build();
    }

    public async Task PublishAsync<T>(
        string topic,
        string key,
        T message,
        string? correlationId = null,
        CancellationToken cancellationToken = default)
    {
        var json = JsonSerializer.Serialize(message, JsonOptions);

        var headers = new Headers();

        if (!string.IsNullOrWhiteSpace(correlationId))
        {
            headers.Add(
                "X-Correlation-ID",
                Encoding.UTF8.GetBytes(correlationId));
        }

        var kafkaMessage = new Message<string, string>
        {
            Key = key,
            Value = json,
            Headers = headers
        };

        try
        {
            _logger.LogInformation(
                "Publishing Kafka event. Topic={Topic}, Key={Key}, EventType={EventType}, CorrelationId={CorrelationId}",
                topic,
                key,
                typeof(T).Name,
                correlationId);

            var result = await _producer.ProduceAsync(
                topic,
                kafkaMessage,
                cancellationToken);

            _logger.LogInformation(
                "Kafka event published successfully. Topic={Topic}, Partition={Partition}, Offset={Offset}, Key={Key}, CorrelationId={CorrelationId}",
                result.Topic,
                result.Partition.Value,
                result.Offset.Value,
                key,
                correlationId);
        }
        catch (ProduceException<string, string> ex)
        {
            _logger.LogError(
                ex,
                "Kafka publish failed. Topic={Topic}, Key={Key}, Error={Error}, CorrelationId={CorrelationId}",
                topic,
                key,
                ex.Error.Reason,
                correlationId);

            throw;
        }
    }

    public void Dispose()
    {
        _producer.Flush(TimeSpan.FromSeconds(10));
        _producer.Dispose();
    }
}
