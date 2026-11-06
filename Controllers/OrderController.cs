using Microsoft.AspNetCore.Mvc;
using System.ComponentModel.DataAnnotations;
using KafkaDemo.Services.Interfaces;

namespace KafkaDemo.Controllers;

[ApiController]
[Route("api/orders")]
public sealed class OrdersController(
    IOrderService orderService) : ControllerBase
{
    [HttpPost]
    public async Task<IActionResult> Create(
        [FromBody] CreateOrderRequest request,
        CancellationToken cancellationToken)
    {
        var order = await orderService.CreateAsync(
            request.CustomerName,
            request.TotalAmount,
            cancellationToken);

        return Accepted(new
        {
            Message = "Order created and event published.",
            OrderId = order.Id,
            CustomerName = order.CustomerName,
            TotalAmount = order.TotalAmount,
            CreatedAt = order.CreatedAt
        });
    }
     // GET: api/orders
    [HttpGet]
    public async Task<IActionResult> GetAll(
        CancellationToken cancellationToken)
    {
        var orders = await orderService.GetAllAsync(
            cancellationToken);

        return Ok(new
        {
            Total = orders.Count,
            Data = orders
        });
    }

    // GET: api/orders/{id}
    [HttpGet("{id:guid}")]
    public async Task<IActionResult> GetById(
        Guid id,
        CancellationToken cancellationToken)
    {
        var order = await orderService.GetByIdAsync(
            id,
            cancellationToken);

        if (order is null)
        {
            return NotFound(new
            {
                Message = "Order not found",
                OrderId = id
            });
        }

        return Ok(order);
    }
    
}


public sealed record CreateOrderRequest(
    [Required]
    [StringLength(200, MinimumLength = 2)]
    string CustomerName,

    [Range(typeof(decimal), "0.01", "999999999")]
    decimal TotalAmount
);