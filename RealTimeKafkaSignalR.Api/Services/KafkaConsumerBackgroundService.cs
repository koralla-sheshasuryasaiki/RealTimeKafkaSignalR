using Confluent.Kafka;
using Microsoft.AspNetCore.SignalR;
using RealTimeKafkaSignalR.Api.Hubs;
using RealTimeKafkaSignalR.Api.Models;
using System.Text.Json;

namespace RealTimeKafkaSignalR.Api.Services
{
    public class KafkaConsumerBackgroundService : BackgroundService
    {
        private const string Topic = "order-events";

        private readonly ILogger<KafkaConsumerBackgroundService> _logger;
        private readonly IHubContext<OrderHub> _hubContext;

        public KafkaConsumerBackgroundService(
            ILogger<KafkaConsumerBackgroundService> logger,
            IHubContext<OrderHub> hubContext)
        {
            _logger = logger;
            _hubContext = hubContext;
        }

        protected override Task ExecuteAsync(CancellationToken stoppingToken)
        {
            return Task.Run(async () =>
            {
                var config = new ConsumerConfig
                {
                    BootstrapServers = "localhost:9092",
                    GroupId = "order-ui-consumer-group",
                    AutoOffsetReset = AutoOffsetReset.Earliest,
                    EnableAutoCommit = true
                };

                using var consumer = new ConsumerBuilder<Ignore, string>(config).Build();
                consumer.Subscribe(Topic);

                _logger.LogInformation("Kafka consumer started. Topic: {Topic}", Topic);

                while (!stoppingToken.IsCancellationRequested)
                {
                    try
                    {
                        var result = consumer.Consume(stoppingToken);

                        var orderEvent = JsonSerializer.Deserialize<OrderEvent>(result.Message.Value);

                        if (orderEvent is null)
                        {
                            continue;
                        }

                        _logger.LogInformation(
                            "Kafka event received. OrderId: {OrderId}, Customer: {CustomerName}, Status: {Status}",
                            orderEvent.OrderId,
                            orderEvent.CustomerName,
                            orderEvent.Status
                        );

                        await _hubContext.Clients.All.SendAsync(
                            "OrderStatusChanged",
                            orderEvent,
                            stoppingToken
                        );
                    }
                    catch (OperationCanceledException)
                    {
                        break;
                    }
                    catch (Exception ex)
                    {
                        _logger.LogError(ex, "Kafka consumer error");
                    }
                }

                consumer.Close();

            }, stoppingToken);
        }
    }
}
