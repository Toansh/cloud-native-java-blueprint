package dev.toansh.blueprint.infrastructure.outbox;

import io.micrometer.tracing.Tracer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.header.internals.RecordHeader;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.data.domain.PageRequest;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.kafka.support.SendResult;
import org.springframework.scheduling.annotation.Scheduled;
import org.springframework.stereotype.Component;
import org.springframework.transaction.annotation.Transactional;

import java.nio.charset.StandardCharsets;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.TimeUnit;

@Component
public class OutboxRelayScheduler {

    private static final Logger log = LoggerFactory.getLogger(OutboxRelayScheduler.class);

    private final OutboxRepository outboxRepository;
    private final KafkaTemplate<String, String> kafkaTemplate;
    private final Tracer tracer;
    private final String targetTopic;
    private final int batchSize;
    private final int maxRetries;

    public OutboxRelayScheduler(
            OutboxRepository outboxRepository,
            KafkaTemplate<String, String> kafkaTemplate,
            Tracer tracer,
            @Value("${app.outbox.topic:order-events}") String targetTopic,
            @Value("${app.outbox.batch-size:100}") int batchSize,
            @Value("${app.outbox.max-retries:5}") int maxRetries) {
        this.outboxRepository = outboxRepository;
        this.kafkaTemplate = kafkaTemplate;
        this.tracer = tracer;
        this.targetTopic = targetTopic;
        this.batchSize = batchSize;
        this.maxRetries = maxRetries;
    }

    @Scheduled(fixedDelayString = "${app.outbox.poll-interval-ms:500}")
    @Transactional
    public void processOutboxMessages() {
        List<OutboxMessage> pendingMessages = outboxRepository.findPendingWithLock(
                OutboxStatus.PENDING,
                maxRetries,
                PageRequest.of(0, batchSize)
        );

        if (pendingMessages.isEmpty()) {
            return;
        }

        log.debug("Found {} pending outbox records to dispatch to Kafka topic '{}'", pendingMessages.size(), targetTopic);

        for (OutboxMessage message : pendingMessages) {
            relaySingleMessage(message);
        }
    }

    private void relaySingleMessage(OutboxMessage message) {
        ProducerRecord<String, String> record = new ProducerRecord<>(
                targetTopic,
                message.getAggregateId(),
                message.getPayload()
        );

        record.headers().add(new RecordHeader("eventId", message.getId().toString().getBytes(StandardCharsets.UTF_8)));
        record.headers().add(new RecordHeader("eventType", message.getEventType().getBytes(StandardCharsets.UTF_8)));
        record.headers().add(new RecordHeader("aggregateType", message.getAggregateType().getBytes(StandardCharsets.UTF_8)));

        var currentSpan = tracer.currentSpan();
        if (currentSpan != null) {
            var context = currentSpan.context();
            if (context != null) {
                String traceParent = "00-%s-%s-%s".formatted(
                        context.traceId(),
                        context.spanId(),
                        context.sampled() != null && context.sampled() ? "01" : "00"
                );
                record.headers().add(new RecordHeader("traceparent", traceParent.getBytes(StandardCharsets.UTF_8)));
            }
        }

        try {
            CompletableFuture<SendResult<String, String>> future = kafkaTemplate.send(record);
            future.get(5, TimeUnit.SECONDS);

            message.markSent();
            log.info("Dispatched outbox event [ID: {}] to topic '{}'", message.getId(), targetTopic);
        } catch (Exception ex) {
            log.warn("Failed to dispatch outbox message [ID: {}], retry attempt {}/{}. Error: {}",
                    message.getId(), message.getRetryCount() + 1, maxRetries, ex.getMessage());
            message.markFailed(ex.getMessage(), maxRetries);
        }
    }
}
