
namespace KafkaDemo.Services.Kafka;

public sealed class KafkaOptions
{
  public string BootstrapServers { get; set; } = "";
    public string OrderCreatedTopic { get; set; } = "";
    public string OrderCreatedDltTopic { get; set; } = "";
    public string ConsumerGroupId { get; set; } = "";

    public int RetryCount { get; set; } = 3;
    public int RetryDelayMilliseconds { get; set; } = 1000;
}
