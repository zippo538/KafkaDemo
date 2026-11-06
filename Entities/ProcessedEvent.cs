using System;

namespace KafkaDemo.Entities;

public class ProcessedEvent
{
    public Guid EventId { get; set; }
    public DateTime ProcessedAt { get; set; }
}
