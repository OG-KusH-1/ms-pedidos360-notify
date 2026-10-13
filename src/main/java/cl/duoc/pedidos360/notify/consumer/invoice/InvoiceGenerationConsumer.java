package cl.duoc.pedidos360.notify.consumer.invoice;

import cl.duoc.pedidos360.notify.model.OrderCommandEnvelope;
import com.rabbitmq.client.Channel;
import lombok.extern.slf4j.Slf4j;
import org.springframework.amqp.rabbit.annotation.RabbitListener;
import org.springframework.amqp.support.AmqpHeaders;
import org.springframework.messaging.handler.annotation.Header;
import org.springframework.stereotype.Component;

import java.io.IOException;

/**
 * InvoiceGenerationConsumer - Consumidor del dominio Facturación / Finanzas.
 *
 * Genera boletas o facturas electrónicas en segundo plano y maneja ACK/NACK.
 */
@Component
@Slf4j
public class InvoiceGenerationConsumer {

    @RabbitListener(
            queues = "${app.rabbitmq.queues.invoice}",
            containerFactory = "rabbitListenerContainerFactory"
    )
    public void processInvoiceCommand(
            OrderCommandEnvelope<?> message,
            Channel channel,
            @Header(AmqpHeaders.DELIVERY_TAG) long deliveryTag) throws IOException {

        log.info("[INVOICE-CONSUMER] <<< Mensaje recibido de q.cmd.invoice. EventId: {}, Pedido: {}",
                message.getEventId(), message.getCorrelationId());

        try {
            if (message.isForceFail()) {
                throw new IllegalStateException("Simulación de fallo en generación de boleta para prueba de DLQ");
            }

            // Procesamiento de factura
            log.info("[INVOICE-CONSUMER] Generando documento tributario electrónico para Pedido #{}...", message.getCorrelationId());
            log.info("[INVOICE-CONSUMER] Boleta electrónica emitida y registrada contablemente.");

            // Confirmación explícita (ACK)
            channel.basicAck(deliveryTag, false);
            log.info("[INVOICE-CONSUMER] >>> ACK enviado con éxito para DeliveryTag: {}", deliveryTag);

        } catch (Exception ex) {
            log.error("[INVOICE-CONSUMER] !!! Error generando factura para orden {}: {}",
                    message.getCorrelationId(), ex.getMessage());

            // basicNack con requeue=false -> mensaje viaja a q.cmd.invoice.dlq
            channel.basicNack(deliveryTag, false, false);
            log.warn("[INVOICE-CONSUMER] >>> NACK emitido. Mensaje transferido a q.cmd.invoice.dlq.");
        }
    }
}
