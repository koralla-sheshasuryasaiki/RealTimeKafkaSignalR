using Confluent.Kafka;
using RealTimeKafkaSignalR.Api.Models;
using System.Text.Json;

namespace RealTimeKafkaSignalR.Api.Services
{
    public class KafkaProducerService
    {
        private const string Topic = "order-events";
        private readonly IProducer<Null, string> _producer;

        public KafkaProducerService()
        {
            var config = new ProducerConfig
            {
                BootstrapServers = "localhost:9092"
            };

            _producer = new ProducerBuilder<Null, string>(config).Build();
        }

        public async Task PublishOrderEventAsync(OrderEvent orderEvent)
        {
            var message = JsonSerializer.Serialize(orderEvent);

            await _producer.ProduceAsync(Topic, new Message<Null, string>
            {
                Value = message
            });
        }

        public void Dispose()
        {
            _producer.Flush(TimeSpan.FromSeconds(5));
            _producer.Dispose();
        }
    }
}
