
namespace KafkaDemo.Entities;

public class Order
{
    public Guid Id { get; set; }

    public string CustomerName { get; set; } = "";

    public decimal TotalAmount { get; set; }

    public DateTime CreatedAt { get; set; }
}
