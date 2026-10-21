package cl.duoc.pedidos360.notify.consumer.dlq;

import cl.duoc.pedidos360.notify.model.OrderCommandEnvelope;
import com.rabbitmq.client.Channel;
import lombok.extern.slf4j.Slf4j;
import org.springframework.amqp.core.Message;
import org.springframework.amqp.rabbit.annotation.RabbitListener;
import org.springframework.amqp.support.AmqpHeaders;
import org.springframework.messaging.handler.annotation.Header;
import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty;
import org.springframework.stereotype.Component;

import java.io.IOException;

/**
 * DeadLetterConsumer - Auditor y monitor de colas de mensajes no entregados (DLQ).
 *
 * NOTA: Por defecto está deshabilitado (@ConditionalOnProperty) para permitir
 * que los mensajes se queden acumulados en la DLQ y el docente pueda ver el contador
 * en '1' e inspeccionar el mensaje en la consola web de RabbitMQ con 'Get Messages'.
 */
@Component
@ConditionalOnProperty(name = "app.rabbitmq.dlq.consumer.enabled", havingValue = "true", matchIfMissing = false)
@Slf4j
public class DeadLetterConsumer {

    @RabbitListener(
            queues = {
                "${app.rabbitmq.queues.email-dlq}",
                "${app.rabbitmq.queues.kitchen-dlq}",
                "${app.rabbitmq.queues.invoice-dlq}"
            },
            containerFactory = "rabbitListenerContainerFactory"
    )
    public void processDeadLetter(
            OrderCommandEnvelope<?> failedEnvelope,
            Message rawMessage,
            Channel channel,
            @Header(AmqpHeaders.DELIVERY_TAG) long deliveryTag,
            @Header(AmqpHeaders.CONSUMER_QUEUE) String queueName) throws IOException {

        log.warn("==========================================================================");
        log.warn("[DLQ-AUDIT] !!! MENSAJE NO ENTREGADO DETECTADO EN DEAD LETTER QUEUE: '{}'", queueName);
        log.warn("[DLQ-AUDIT] EventId: {}", failedEnvelope.getEventId());
        log.warn("[DLQ-AUDIT] Tipo de Comando: {}", failedEnvelope.getType());
        log.warn("[DLQ-AUDIT] CorrelationId (Pedido): {}", failedEnvelope.getCorrelationId());
        log.warn("[DLQ-AUDIT] TraceId: {}", failedEnvelope.getTraceId());
        log.warn("[DLQ-AUDIT] Carga Útil (Payload): {}", failedEnvelope.getPayload());
        log.warn("[DLQ-AUDIT] Metadatos de fallo (Headers): {}", rawMessage.getMessageProperties().getHeaders());
        log.warn("==========================================================================");

        // Confirmamos lectura del log de auditoría
        channel.basicAck(deliveryTag, false);
    }
}
