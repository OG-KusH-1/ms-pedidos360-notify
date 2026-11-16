package cl.duoc.pedidos360.notify.streaming.dto;

import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.NoArgsConstructor;

import java.time.Instant;

@Data
@Builder
@NoArgsConstructor
@AllArgsConstructor
public class OrderStreamEventDTO {
    private String eventId;
    private String eventType;
    private Long orderId;
    private String descripcion;
    private String estadoAnterior;
    private String estadoNuevo;
    private String usuario;
    private Instant timestamp;
    private boolean simulateError;
}
