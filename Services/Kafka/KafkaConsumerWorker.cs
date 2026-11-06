using System.Text;
using System.Text.Json;
using Confluent.Kafka;
using KafkaDemo.Contracts;
using KafkaDemo.Data;
using KafkaDemo.Entities;
using KafkaDemo.Services.Interfaces;
using Microsoft.EntityFrameworkCore;
using Serilog.Context;

namespace KafkaDemo.Services.Kafka;

public sealed class KafkaConsumerWorker : BackgroundService
{
    private readonly KafkaOptions _options;
    private readonly IServiceScopeFactory _scopeFactory;
    private readonly IKafkaProducer _producer;
    private readonly ILogger<KafkaConsumerWorker> _logger;

    private static readonly JsonSerializerOptions JsonOptions =
        new(JsonSerializerDefaults.Web);

    public KafkaConsumerWorker(
        KafkaOptions options,
        IServiceScopeFactory scopeFactory,
        IKafkaProducer producer,
        ILogger<KafkaConsumerWorker> logger)
    {
        _options = options;
        _scopeFactory = scopeFactory;
        _producer = producer;
        _logger = logger;
    }

    protected override async Task ExecuteAsync(
        CancellationToken stoppingToken)
    {
        var config = new ConsumerConfig
        {
            BootstrapServers = _options.BootstrapServers,
            GroupId = _options.ConsumerGroupId,
            AutoOffsetReset = AutoOffsetReset.Earliest,
            EnableAutoCommit = false,
            EnableAutoOffsetStore = false,
            AllowAutoCreateTopics = false,
            SessionTimeoutMs = 10000,
            MaxPollIntervalMs = 300000
        };

        using var consumer =
            new ConsumerBuilder<string, string>(config)
                .SetErrorHandler((_, error) =>
                {
                    _logger.LogError(
                        "Kafka consumer error: {Reason}",
                        error.Reason);
                })
                .Build();

        consumer.Subscribe(_options.OrderCreatedTopic);

        _logger.LogInformation(
            "Kafka consumer started. Topic={Topic}, Group={Group}",
            _options.OrderCreatedTopic,
            _options.ConsumerGroupId);

        try
        {
            while (!stoppingToken.IsCancellationRequested)
            {
                ConsumeResult<string, string>? result = null;

                try
                {
                    result = consumer.Consume(stoppingToken);

                    if (result.IsPartitionEOF)
                    {
                        continue;
                    }

                    await ProcessWithRetryAsync(
                        consumer,
                        result,
                        stoppingToken);
                }
                catch (OperationCanceledException)
                    when (stoppingToken.IsCancellationRequested)
                {
                    break;
                }
                catch (ConsumeException ex)
                {
                    _logger.LogError(
                        ex,
                        "Kafka consume error. Code={Code}, Reason={Reason}",
                        ex.Error.Code,
                        ex.Error.Reason);
                        try
                        {
                            await Task.Delay(
                                TimeSpan.FromSeconds(5),
                                stoppingToken);
                        }
                        catch (OperationCanceledException)
                        when (stoppingToken.IsCancellationRequested)
                        {
                            break;
                        }
                }
                catch (Exception ex)
                {
                    _logger.LogError(
                        ex,
                        "Message handling failed. Topic={Topic}, Offset={Offset}",
                        result?.Topic,
                        result?.Offset.Value);

                    if (result is not null)
                    {
                        // Do not commit the failed message. Seek back so it
                        // can be consumed again after a short delay.
                        consumer.Seek(result.TopicPartitionOffset);

                        try
                        {
                            await Task.Delay(
                                TimeSpan.FromSeconds(2),
                                stoppingToken);
                        }
                        catch (OperationCanceledException)
                            when (stoppingToken.IsCancellationRequested)
                        {
                            break;
                        }
                    }
                }
            }
        }
        finally
        {
            consumer.Close();
            _logger.LogInformation("Kafka consumer stopped.");
        }
    }

    private async Task ProcessWithRetryAsync(
        IConsumer<string, string> consumer,
        ConsumeResult<string, string> result,
        CancellationToken cancellationToken)
    {
        Exception? lastException = null;
        var correlationId = ReadCorrelationId(result.Message.Headers);

        using var topicContext =
            LogContext.PushProperty("KafkaTopic", result.Topic);
        using var partitionContext =
            LogContext.PushProperty("KafkaPartition", result.Partition.Value);
        using var offsetContext =
            LogContext.PushProperty("KafkaOffset", result.Offset.Value);
        using var correlationContext =
            LogContext.PushProperty("CorrelationId", correlationId);

        for (var attempt = 1;
             attempt <= _options.RetryCount;
             attempt++)
        {
            try
            {
                await ProcessOrderAsync(
                    result.Message.Value,
                    cancellationToken);

                consumer.Commit(result);

                _logger.LogInformation(
                    "Message processed. Offset={Offset}, Attempt={Attempt}",
                    result.Offset.Value,
                    attempt);

                return;
            }
            catch (OperationCanceledException)
                when (cancellationToken.IsCancellationRequested)
            {
                throw;
            }
            catch (Exception ex)
            {
                lastException = ex;

                _logger.LogWarning(
                    ex,
                    "Processing attempt {Attempt}/{MaxAttempt} failed",
                    attempt,
                    _options.RetryCount);

                if (attempt < _options.RetryCount)
                {
                    var delay = TimeSpan.FromMilliseconds(
                        _options.RetryDelayMilliseconds *
                        Math.Pow(2, attempt - 1));

                    await Task.Delay(delay, cancellationToken);
                }
            }
        }

        var dltMessage = new
        {
            OriginalTopic = result.Topic,
            OriginalPartition = result.Partition.Value,
            OriginalOffset = result.Offset.Value,
            OriginalKey = result.Message.Key,
            OriginalMessage = result.Message.Value,
            Error = lastException?.ToString(),
            FailedAt = DateTime.UtcNow
        };

        // Publish to DLT first. Commit the source offset only after Kafka
        // acknowledges the DLT publication.
        await _producer.PublishAsync(
            _options.OrderCreatedDltTopic,
            result.Message.Key ?? Guid.NewGuid().ToString(),
            dltMessage,
            correlationId,
            cancellationToken);

        consumer.Commit(result);

        _logger.LogError(
            "Message moved to DLT. Offset={Offset}",
            result.Offset.Value);
    }

    private static string? ReadCorrelationId(Headers? headers)
    {
        if (headers is null)
        {
            return null;
        }

        var correlationIdBytes = headers
            .Where(header => header.Key == "X-Correlation-ID")
            .Select(header => header.GetValueBytes())
            .LastOrDefault();

        return correlationIdBytes is { Length: > 0 }
            ? Encoding.UTF8.GetString(correlationIdBytes)
            : null;
    }

    private async Task ProcessOrderAsync(
        string message,
        CancellationToken cancellationToken)
    {
        var orderEvent =
            JsonSerializer.Deserialize<OrderCreatedEvent>(
                message,
                JsonOptions);

        if (orderEvent is null ||
            orderEvent.EventId == Guid.Empty ||
            orderEvent.OrderId == Guid.Empty)
        {
            throw new InvalidOperationException(
                "Invalid OrderCreated event.");
        }

        await using var scope = _scopeFactory.CreateAsyncScope();

        var db = scope.ServiceProvider
            .GetRequiredService<AppDbContext>();

        var alreadyProcessed =
            await db.ProcessedEvents.AnyAsync(
                x => x.EventId == orderEvent.EventId,
                cancellationToken);

        if (alreadyProcessed)
        {
            _logger.LogWarning(
                "Duplicate event ignored. EventId={EventId}",
                orderEvent.EventId);

            return;
        }

        _logger.LogInformation(
            "Processing order. OrderId={OrderId}, Customer={Customer}, Amount={Amount}",
            orderEvent.OrderId,
            orderEvent.CustomerName,
            orderEvent.TotalAmount);

        // Place real business logic here:
        // - Update inventory
        // - Create invoice
        // - Send notification

        db.ProcessedEvents.Add(new ProcessedEvent
        {
            EventId = orderEvent.EventId,
            ProcessedAt = DateTime.UtcNow
        });

        await db.SaveChangesAsync(cancellationToken);
    }
}
