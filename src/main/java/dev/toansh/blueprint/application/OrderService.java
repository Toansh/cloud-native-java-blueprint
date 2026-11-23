package dev.toansh.blueprint.application;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import dev.toansh.blueprint.domain.model.Order;
import dev.toansh.blueprint.domain.repository.OrderRepository;
import dev.toansh.blueprint.infrastructure.outbox.OutboxMessage;
import dev.toansh.blueprint.infrastructure.outbox.OutboxRepository;
import dev.toansh.blueprint.infrastructure.web.dto.CreateOrderRequest;
import dev.toansh.blueprint.infrastructure.web.dto.OrderResponse;
import io.github.resilience4j.circuitbreaker.annotation.CircuitBreaker;
import io.micrometer.core.instrument.Counter;
import io.micrometer.core.instrument.MeterRegistry;
import io.micrometer.core.instrument.Timer;
import jakarta.persistence.EntityNotFoundException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Transactional;

import java.util.UUID;

@Service
public class OrderService {

    private static final Logger log = LoggerFactory.getLogger(OrderService.class);
    private static final String RESILIENCE_INSTANCE = "orderService";

    private final OrderRepository orderRepository;
    private final OutboxRepository outboxRepository;
    private final ObjectMapper objectMapper;
    private final Counter orderCreatedCounter;
    private final Timer orderCreationTimer;

    public OrderService(OrderRepository orderRepository,
                        OutboxRepository outboxRepository,
                        ObjectMapper objectMapper,
                        MeterRegistry meterRegistry) {
        this.orderRepository = orderRepository;
        this.outboxRepository = outboxRepository;
        this.objectMapper = objectMapper;

        this.orderCreatedCounter = Counter.builder("business.orders.created.count")
                .description("Total number of orders created successfully")
                .register(meterRegistry);

        this.orderCreationTimer = Timer.builder("business.orders.creation.duration")
                .description("Latency distribution for order creation workflow")
                .register(meterRegistry);
    }

    @Transactional
    @CircuitBreaker(name = RESILIENCE_INSTANCE, fallbackMethod = "createOrderFallback")
    public OrderResponse createOrder(CreateOrderRequest request) {
        return orderCreationTimer.record(() -> {
            log.info("Initiating atomic order creation for customer: {}", request.customerId());

            Order order = Order.create(request.customerId(), request.amount(), request.currency());
            Order savedOrder = orderRepository.save(order);

            OrderResponse response = OrderResponse.fromDomain(savedOrder);
            String payloadJson;
            try {
                payloadJson = objectMapper.writeValueAsString(response);
            } catch (JsonProcessingException e) {
                throw new IllegalStateException("Failed to serialize order event to JSON", e);
            }

            OutboxMessage outboxMessage = OutboxMessage.builder()
                    .aggregateType("ORDER")
                    .aggregateId(savedOrder.getId().toString())
                    .eventType("ORDER_CREATED")
                    .payload(payloadJson)
                    .build();

            outboxRepository.save(outboxMessage);
            orderCreatedCounter.increment();

            log.info("Order [{}] and OutboxMessage [{}] persisted transactionally", savedOrder.getId(), outboxMessage.getId());
            return response;
        });
    }

    @Transactional(readOnly = true)
    @CircuitBreaker(name = RESILIENCE_INSTANCE)
    public OrderResponse getOrder(UUID orderId) {
        return orderRepository.findById(orderId)
                .map(OrderResponse::fromDomain)
                .orElseThrow(() -> new EntityNotFoundException("Order with ID " + orderId + " was not found."));
    }

    public OrderResponse createOrderFallback(CreateOrderRequest request, Throwable ex) {
        log.error("Order creation circuit breaker triggered for customer: {}. Reason: {}",
                request.customerId(), ex.getMessage());
        throw new IllegalStateException("Order processing service is currently degraded. Please retry later.", ex);
    }
}
