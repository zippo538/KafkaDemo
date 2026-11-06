using KafkaDemo.Services;
using Serilog.Context;

namespace KafkaDemo.Middlewares;

public sealed class CorrelationIdMiddleware
{
    private readonly RequestDelegate _next;

    public CorrelationIdMiddleware(RequestDelegate next)
    {
        _next = next;
    }

    public async Task InvokeAsync(
        HttpContext context,
        CorrelationContext correlation)
    {
        var id = context.Request.Headers["X-Correlation-ID"]
            .FirstOrDefault();

        if (string.IsNullOrWhiteSpace(id) ||
            id.Length > 128 ||
            id.Any(char.IsControl))
        {
            id = Guid.NewGuid().ToString("N");
        }

        correlation.Id = id;

        context.Response.Headers["X-Correlation-ID"] = id;

        using (LogContext.PushProperty("CorrelationId", id))
        {
            await _next(context);
        }
    }
}