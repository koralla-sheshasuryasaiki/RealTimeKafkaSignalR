using Confluent.Kafka;
using Microsoft.AspNetCore.Http;
using Microsoft.AspNetCore.Mvc;
using Microsoft.AspNetCore.SignalR;
using RealTimeKafkaSignalR.Api.Hubs;
using RealTimeKafkaSignalR.Api.Models;
using RealTimeKafkaSignalR.Api.Services;

namespace RealTimeKafkaSignalR.Api.Controllers
{
   
    [ApiController]
    [Route("api/orders")]
    public class OrderController : ControllerBase
    {
        private readonly KafkaProducerService _producerService;       
        private readonly ILogger<OrderController> _logger;
        private readonly IHubContext<OrderHub> _hubContext;
        private readonly IConfiguration _configuration;

        public OrderController(
            ILogger<OrderController> logger,
            IHubContext<OrderHub> hubContext,
            IConfiguration configuration,
            KafkaProducerService? producerService = null)
        {
            _logger = logger;
            _hubContext = hubContext;
            _configuration = configuration;
            _producerService = producerService;
        }

        //public OrderController(KafkaProducerService producerService = null)
        //{
        //    _producerService = producerService;
        //}

        [HttpPost]
        public async Task<IActionResult> CreateOrder(CreateOrderRequest request)
        {
            var kafkaEnabled = _configuration.GetValue<bool>("Kafka:Enabled");

            var orderEvent = new OrderEvent
            {
                OrderId = Guid.NewGuid(),
                CustomerName = request.CustomerName,
                Status = "Created",
                CreatedAt = DateTime.UtcNow
            };

            if (kafkaEnabled && _producerService is not null)
            {
                await _producerService.PublishOrderEventAsync(orderEvent);
                _logger.LogInformation(
                    "Order API called and Kafka event published. OrderId: {OrderId}, Customer: {CustomerName}",
                    orderEvent.OrderId,
                    orderEvent.CustomerName
                );
            }
            else
            {
                await _hubContext.Clients.All.SendAsync("OrderStatusChanged", orderEvent);

                _logger.LogInformation(
                    "Order API called. Kafka disabled. SignalR event sent directly. OrderId: {OrderId}, Customer: {CustomerName}",
                    orderEvent.OrderId,
                    orderEvent.CustomerName
                );
            }
            //if (string.IsNullOrWhiteSpace(request.CustomerName))
            //{
            //    return BadRequest("Customer name is required");
            //}

            //var orderEvent = new OrderEvent
            //{
            //    OrderId = Guid.NewGuid(),
            //    CustomerName = request.CustomerName,
            //    Status = "Created",
            //    CreatedAt = DateTime.UtcNow
            //};

            //await _producerService.PublishOrderEventAsync(orderEvent);

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
