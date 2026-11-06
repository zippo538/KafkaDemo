using KafkaDemo.Entities;
using Microsoft.EntityFrameworkCore;

namespace KafkaDemo.Data;

public sealed class AppDbContext(
    DbContextOptions<AppDbContext> options)
    : DbContext(options)
{
    public DbSet<Order> Orders => Set<Order>();

    public DbSet<ProcessedEvent> ProcessedEvents
        => Set<ProcessedEvent>();

    protected override void OnModelCreating(ModelBuilder modelBuilder)
    {
        modelBuilder.Entity<Order>(entity =>
        {
            entity.HasKey(x => x.Id);

            entity.Property(x => x.CustomerName)
                .HasMaxLength(200)
                .IsRequired();

            entity.Property(x => x.TotalAmount)
                .HasPrecision(18, 2);
        });

        modelBuilder.Entity<ProcessedEvent>(entity =>
        {
            entity.HasKey(x => x.EventId);
        });
    }
}