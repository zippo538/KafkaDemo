namespace KafkaDemo.Contracts;

public class OrderCreatedEvent
{
    public Guid EventId { get; init; }

    public Guid OrderId { get; set; }
    public string CustomerName { get; set; } = "";
    public decimal TotalAmount { get; set; }
    public DateTime CreatedAt { get; set; }
}