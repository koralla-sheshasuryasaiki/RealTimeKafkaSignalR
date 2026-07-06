namespace RealTimeKafkaSignalR.Api.Models
{
    public class OrderEvent
    {
        public Guid OrderId { get; set; }
        public string CustomerName { get; set; } = string.Empty;
        public string Status { get; set; } = "Created";
        public DateTime CreatedAt { get; set; }
    }
}
