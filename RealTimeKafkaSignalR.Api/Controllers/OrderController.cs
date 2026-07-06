using Microsoft.AspNetCore.Http;
using Microsoft.AspNetCore.Mvc;
using RealTimeKafkaSignalR.Api.Models;
using RealTimeKafkaSignalR.Api.Services;

namespace RealTimeKafkaSignalR.Api.Controllers
{
   
    [ApiController]
    [Route("api/orders")]
    public class OrderController : ControllerBase
    {
        private readonly KafkaProducerService _producerService;

        public OrderController(KafkaProducerService producerService)
        {
            _producerService = producerService;
        }

        [HttpPost]
        public async Task<IActionResult> CreateOrder(CreateOrderRequest request)
        {
            if (string.IsNullOrWhiteSpace(request.CustomerName))
            {
                return BadRequest("Customer name is required");
            }

            var orderEvent = new OrderEvent
            {
                OrderId = Guid.NewGuid(),
                CustomerName = request.CustomerName,
                Status = "Created",
                CreatedAt = DateTime.UtcNow
            };

            await _producerService.PublishOrderEventAsync(orderEvent);

            return Ok(new
            {
                message = "Order created and Kafka event published",
                order = orderEvent
            });
        }

        public class CreateOrderRequest
        {
            public string CustomerName { get; set; } = string.Empty;
        }
    }
}
