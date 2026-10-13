package cl.duoc.pedidos360.notify.model;

import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.NoArgsConstructor;

import java.io.Serializable;
import java.time.Instant;

/**
 * OrderCommandEnvelope - Modelo de transporte común recibido desde RabbitMQ.
 */
@Data
@Builder
@NoArgsConstructor
@AllArgsConstructor
@JsonIgnoreProperties(ignoreUnknown = true)
public class OrderCommandEnvelope<T> implements Serializable {

    private String type;
    private String eventId;
    private Instant timestamp;
    private String traceId;
    private String correlationId;
    private T payload;
    private boolean forceFail;
}
