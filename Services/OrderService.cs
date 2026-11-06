using KafkaDemo.Contracts;
using KafkaDemo.Data;
using KafkaDemo.Entities;
using KafkaDemo.Services.Interfaces;
using KafkaDemo.Services.Kafka;
using Microsoft.EntityFrameworkCore;
using Microsoft.Extensions.Options;

namespace KafkaDemo.Services;

public sealed class OrderService : IOrderService
{
 private readonly AppDbContext _db;
    private readonly IKafkaProducer _producer;
    private readonly KafkaOptions _options;
    private readonly CorrelationContext _correlation;


    public OrderService(
         AppDbContext db,
    IKafkaProducer producer,
    IOptions<KafkaOptions> options,
    CorrelationContext correlation)
    {
        _db = db;
    _producer = producer;
    _options = options.Value;
    _correlation = correlation;
    }

    public async Task<Order> CreateAsync(
        string customerName,
        decimal totalAmount,
        CancellationToken cancellationToken)
    {
        var order = new Order
        {
            Id = Guid.NewGuid(),
            CustomerName = customerName,
            TotalAmount = totalAmount,
            CreatedAt = DateTime.UtcNow
        };

        _db.Orders.Add(order);

        await _db.SaveChangesAsync(cancellationToken);

        var orderEvent = new OrderCreatedEvent
        {
            EventId = Guid.NewGuid(),
            OrderId = order.Id,
            CustomerName = order.CustomerName,
            TotalAmount = order.TotalAmount,
            CreatedAt = order.CreatedAt
        };

        await _producer.PublishAsync(
            _options.OrderCreatedTopic,
            order.Id.ToString(),
            orderEvent,
            _correlation.Id,
            cancellationToken);

        return order;
    }
    public async Task<List<Order>> GetAllAsync(
    CancellationToken cancellationToken)
    {
    return await _db.Orders
        .AsNoTracking()
        .OrderByDescending(x => x.CreatedAt)
        .ToListAsync(cancellationToken);
    }
    public async Task<Order?> GetByIdAsync(
        Guid id,
        CancellationToken cancellationToken)
        {
            return await _db.Orders
            .AsNoTracking()
            .FirstOrDefaultAsync(x => x.Id == id,cancellationToken);
        }
}
