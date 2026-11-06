
namespace KafkaDemo.Services.Interfaces;

public interface IKafkaProducer
{
    Task PublishAsync<T>(
        string topic,
        string key,
        T message,
        string? correlationId = null,
        CancellationToken cancellationToken = default);
}
