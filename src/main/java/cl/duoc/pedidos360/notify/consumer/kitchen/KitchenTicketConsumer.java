package cl.duoc.pedidos360.notify.consumer.kitchen;

import cl.duoc.pedidos360.notify.model.OrderCommandEnvelope;
import com.rabbitmq.client.Channel;
import lombok.extern.slf4j.Slf4j;
import org.springframework.amqp.rabbit.annotation.RabbitListener;
import org.springframework.amqp.support.AmqpHeaders;
import org.springframework.messaging.handler.annotation.Header;
import org.springframework.stereotype.Component;

import java.io.IOException;

/**
 * KitchenTicketConsumer - Consumidor del dominio Cocina / Preparación.
 *
 * Procesa la comanda de cocina de forma asíncrona y gestiona ACK/NACK explícito.
 */
@Component
@Slf4j
public class KitchenTicketConsumer {

    @RabbitListener(
            queues = "${app.rabbitmq.queues.kitchen}",
            containerFactory = "rabbitListenerContainerFactory"
    )
    public void processKitchenCommand(
            OrderCommandEnvelope<?> message,
            Channel channel,
            @Header(AmqpHeaders.DELIVERY_TAG) long deliveryTag) throws IOException {

        log.info("[KITCHEN-CONSUMER] <<< Mensaje recibido de q.cmd.kitchen. EventId: {}, Pedido: {}",
                message.getEventId(), message.getCorrelationId());

        try {
            if (message.isForceFail()) {
                throw new IllegalStateException("Simulación de fallo de impresión en comanda de cocina para prueba de DLQ");
            }

            // Procesamiento de comanda
            log.info("[KITCHEN-CONSUMER] Generando ticket de preparación para Pedido #{}...", message.getCorrelationId());
            log.info("[KITCHEN-CONSUMER] Comanda impresa y asignada a estación de cocina.");

            // Confirmación explícita (ACK)
            channel.basicAck(deliveryTag, false);
            log.info("[KITCHEN-CONSUMER] >>> ACK enviado con éxito para DeliveryTag: {}", deliveryTag);

        } catch (Exception ex) {
            log.error("[KITCHEN-CONSUMER] !!! Error en ticket de cocina para orden {}: {}",
                    message.getCorrelationId(), ex.getMessage());

            // basicNack con requeue=false -> mensaje viaja a q.cmd.kitchen.dlq
            channel.basicNack(deliveryTag, false, false);
            log.warn("[KITCHEN-CONSUMER] >>> NACK emitido. Mensaje transferido a q.cmd.kitchen.dlq.");
        }
    }
}
