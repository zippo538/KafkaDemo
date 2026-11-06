using KafkaDemo.Entities;

namespace KafkaDemo.Services.Interfaces;

public interface IOrderService
{
    Task<Order> CreateAsync(string customerName, decimal totalAmount,CancellationToken cancellationToken);
    Task<List<Order>> GetAllAsync(CancellationToken cancellationToken);
    Task<Order?> GetByIdAsync(Guid id,CancellationToken cancellationToken);
}
