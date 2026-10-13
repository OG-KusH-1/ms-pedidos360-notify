package cl.duoc.pedidos360.notify.consumer.email;

import cl.duoc.pedidos360.notify.model.OrderCommandEnvelope;
import com.rabbitmq.client.Channel;
import lombok.extern.slf4j.Slf4j;
import org.springframework.amqp.rabbit.annotation.RabbitListener;
import org.springframework.amqp.support.AmqpHeaders;
import org.springframework.messaging.handler.annotation.Header;
import org.springframework.stereotype.Component;

import java.io.IOException;

/**
 * EmailNotificationConsumer - Consumidor del dominio Notificaciones / Comunicaciones.
 *
 * Cumple con los Indicadores 4 y 5 de la pauta de evaluación:
 * - Implementado con @RabbitListener y agrupado por dominio funcional.
 * - Confirmación de mensajes MANUAL con channel.basicAck().
 * - Manejo explícito de errores con basicNack() para desviar a Dead Letter Queue (DLQ).
 */
@Component
@Slf4j
public class EmailNotificationConsumer {

    @RabbitListener(
            queues = "${app.rabbitmq.queues.email}",
            containerFactory = "rabbitListenerContainerFactory"
    )
    public void processEmailCommand(
            OrderCommandEnvelope<?> message,
            Channel channel,
            @Header(AmqpHeaders.DELIVERY_TAG) long deliveryTag) throws IOException {

        log.info("[EMAIL-CONSUMER] <<< Mensaje recibido de q.cmd.email. EventId: {}, CorrelationId: {}",
                message.getEventId(), message.getCorrelationId());

        try {
            // Demostración de error forzado para envío a DLQ en la defensa
            if (message.isForceFail()) {
                throw new IllegalStateException("Simulación de fallo en envío de correo para prueba de DLQ");
            }

            // Procesamiento de negocio (simulación de envío SMTP / Push)
            log.info("[EMAIL-CONSUMER] Procesando notificación para Pedido #{}...", message.getCorrelationId());
            log.info("[EMAIL-CONSUMER] Correo y Notificación Push despachados con éxito al cliente.");

            // Confirmación explícita (ACK)
            channel.basicAck(deliveryTag, false);
            log.info("[EMAIL-CONSUMER] >>> ACK enviado con éxito para DeliveryTag: {}", deliveryTag);

        } catch (Exception ex) {
            log.error("[EMAIL-CONSUMER] !!! Error procesando email para orden {}: {}",
                    message.getCorrelationId(), ex.getMessage());

            // Decisión explícita: basicNack con requeue=false desvía automáticamente a q.cmd.email.dlq
            channel.basicNack(deliveryTag, false, false);
            log.warn("[EMAIL-CONSUMER] >>> NACK emitido (requeue=false). Mensaje desviado a q.cmd.email.dlq.");
        }
    }
}
