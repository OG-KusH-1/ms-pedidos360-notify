package cl.duoc.pedidos360.notify.streaming.consumer;

import cl.duoc.pedidos360.notify.streaming.dto.AnalyticsMetricEventDTO;
import cl.duoc.pedidos360.notify.streaming.dto.OrderStreamEventDTO;
import cl.duoc.pedidos360.notify.streaming.service.AuditDomainService;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.kafka.support.Acknowledgment;
import org.springframework.kafka.support.KafkaHeaders;
import org.springframework.messaging.handler.annotation.Header;
import org.springframework.messaging.handler.annotation.Payload;
import org.springframework.stereotype.Component;

/**
 * Consumidor Kafka independiente en ms-pedidos360-notify (Consumer Group: cg-audit-logger).
 * Demuestra cómo múltiples Consumer Groups leen los mismos tópicos sin competir entre sí.
 */
@Component
@RequiredArgsConstructor
@Slf4j
public class OrderAuditKafkaConsumer {

    private final AuditDomainService auditDomainService;

    @KafkaListener(
            topics = "${app.kafka.topics.orders-events}",
            groupId = "cg-audit-logger",
            containerFactory = "auditOrderKafkaFactory"
    )
    public void consumeOrderAudit(
            @Payload OrderStreamEventDTO event,
            @Header(KafkaHeaders.RECEIVED_PARTITION) int partition,
            @Header(KafkaHeaders.OFFSET) long offset,
            Acknowledgment acknowledgment) {

        auditDomainService.auditOrderEvent(event, partition, offset);
        acknowledgment.acknowledge();
    }

    @KafkaListener(
            topics = "${app.kafka.topics.analytics-metrics}",
            groupId = "cg-audit-logger",
            containerFactory = "auditMetricKafkaFactory"
    )
    public void consumeCatalogMetricAudit(
            @Payload AnalyticsMetricEventDTO metric,
            @Header(KafkaHeaders.RECEIVED_PARTITION) int partition,
            @Header(KafkaHeaders.OFFSET) long offset,
            Acknowledgment acknowledgment) {

        auditDomainService.auditCatalogMetric(metric, partition, offset);
        acknowledgment.acknowledge();
    }
}
