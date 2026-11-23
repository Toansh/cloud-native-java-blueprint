package dev.toansh.blueprint;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import dev.toansh.blueprint.application.OrderService;
import dev.toansh.blueprint.infrastructure.outbox.OutboxRelayScheduler;
import dev.toansh.blueprint.infrastructure.web.dto.CreateOrderRequest;
import dev.toansh.blueprint.infrastructure.web.dto.OrderResponse;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.test.context.DynamicPropertyRegistry;
import org.springframework.test.context.DynamicPropertySource;
import org.testcontainers.containers.KafkaContainer;
import org.testcontainers.containers.PostgreSQLContainer;
import org.testcontainers.junit.jupiter.Container;
import org.testcontainers.junit.jupiter.Testcontainers;
import org.testcontainers.utility.DockerImageName;

import java.math.BigDecimal;
import java.time.Duration;
import java.util.Collections;
import java.util.Map;
import java.util.Properties;
import java.util.UUID;
import java.util.concurrent.TimeUnit;

import static org.assertj.core.api.Assertions.assertThat;
import static org.awaitility.Awaitility.await;

@SpringBootTest(webEnvironment = SpringBootTest.WebEnvironment.RANDOM_PORT)
@Testcontainers
class OutboxPatternIT {

    private static final String ORDERS_TOPIC = "order-events";

    @Container
    static final PostgreSQLContainer<?> postgres = new PostgreSQLContainer<>(DockerImageName.parse("postgres:16-alpine"))
            .withDatabaseName("test_db")
            .withUsername("test_user")
            .withPassword("test_password");

    @Container
    static final KafkaContainer kafka = new KafkaContainer(DockerImageName.parse("confluentinc/cp-kafka:7.6.0"));

    @DynamicPropertySource
    static void configureProperties(DynamicPropertyRegistry registry) {
        registry.add("spring.datasource.url", postgres::getJdbcUrl);
        registry.add("spring.datasource.username", postgres::getUsername);
        registry.add("spring.datasource.password", postgres::getPassword);
        registry.add("spring.kafka.bootstrap-servers", kafka::getBootstrapServers);
        registry.add("app.outbox.topic", () -> ORDERS_TOPIC);
    }

    @Autowired
    private JdbcTemplate jdbcTemplate;

    @Autowired
    private ObjectMapper objectMapper;

    @Autowired
    private OrderService orderService;

    @Autowired
    private OutboxRelayScheduler outboxRelayScheduler;

    private KafkaConsumer<String, String> testKafkaConsumer;

    @BeforeEach
    void setUpConsumer() {
        Properties props = new Properties();
        props.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, kafka.getBootstrapServers());
        props.put(ConsumerConfig.GROUP_ID_CONFIG, "outbox-it-test-group-" + UUID.randomUUID());
        props.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
        props.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        props.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());

        testKafkaConsumer = new KafkaConsumer<>(props);
        testKafkaConsumer.subscribe(Collections.singletonList(ORDERS_TOPIC));
    }

    @AfterEach
    void tearDown() {
        if (testKafkaConsumer != null) {
            testKafkaConsumer.close();
        }
        jdbcTemplate.execute("DELETE FROM outbox_messages");
        jdbcTemplate.execute("DELETE FROM orders");
    }

    @Test
    @DisplayName("Should persist order, create outbox record, and dispatch event to Kafka with status update")
    void shouldProcessOrderThroughOutboxPatternSuccessfully() {
        UUID customerId = UUID.randomUUID();
        BigDecimal amount = new BigDecimal("149.9900");
        String currency = "USD";

        CreateOrderRequest request = new CreateOrderRequest(customerId, amount, currency);
        OrderResponse response = orderService.createOrder(request);

        assertThat(response).isNotNull();
        UUID orderId = response.id();

        // 1. Verify Order persisted in database
        Map<String, Object> orderRecord = jdbcTemplate.queryForMap(
                "SELECT * FROM orders WHERE id = ?", orderId
        );
        assertThat(orderRecord).isNotEmpty();
        assertThat(orderRecord.get("customer_id")).isEqualTo(customerId);

        // 2. Verify Outbox record created with PENDING
        Map<String, Object> outboxRecord = jdbcTemplate.queryForMap(
                "SELECT * FROM outbox_messages WHERE aggregate_id = ?", orderId.toString()
        );
        assertThat(outboxRecord).isNotEmpty();
        assertThat(outboxRecord.get("aggregate_type")).isEqualTo("ORDER");
        assertThat(outboxRecord.get("status")).isEqualTo("PENDING");

        // 3. Trigger Outbox Relay Scheduler
        outboxRelayScheduler.processOutboxMessages();

        // 4. Verify Outbox status transitioned to SENT
        await()
                .atMost(10, TimeUnit.SECONDS)
                .pollInterval(Duration.ofMillis(200))
                .untilAsserted(() -> {
                    Map<String, Object> updatedOutbox = jdbcTemplate.queryForMap(
                            "SELECT status, processed_at FROM outbox_messages WHERE aggregate_id = ?",
                            orderId.toString()
                    );
                    assertThat(updatedOutbox.get("status")).isEqualTo("SENT");
                    assertThat(updatedOutbox.get("processed_at")).isNotNull();
                });

        // 5. Consume from Kafka topic and verify payload
        await()
                .atMost(15, TimeUnit.SECONDS)
                .pollInterval(Duration.ofMillis(500))
                .untilAsserted(() -> {
                    var records = testKafkaConsumer.poll(Duration.ofMillis(500));
                    for (ConsumerRecord<String, String> record : records) {
                        JsonNode root = objectMapper.readTree(record.value());
                        if (orderId.toString().equals(root.path("id").asText())) {
                            assertThat(root.path("customerId").asText()).isEqualTo(customerId.toString());
                            assertThat(root.path("currency").asText()).isEqualTo(currency);
                            return;
                        }
                    }
                    assertThat(records.isEmpty()).isFalse();
                });
    }
}
