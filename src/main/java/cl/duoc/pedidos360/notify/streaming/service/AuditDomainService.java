package cl.duoc.pedidos360.notify.streaming.service;

import cl.duoc.pedidos360.notify.streaming.dto.AnalyticsMetricEventDTO;
import cl.duoc.pedidos360.notify.streaming.dto.OrderStreamEventDTO;
import lombok.extern.slf4j.Slf4j;
import org.springframework.stereotype.Service;

/**
 * Servicio de dominio de Auditoría y Trazabilidad.
 * Mantiene la lógica de negocio completamente separada de los listeners @KafkaListener.
 */
@Service
@Slf4j
public class AuditDomainService {

    public void auditOrderEvent(OrderStreamEventDTO event, int partition, long offset) {
        if (event == null || event.getOrderId() == null || event.getEventType() == null) {
            throw new IllegalArgumentException("Evento de orden nulo o sin identificadores requeridos");
        }
        if (event.isSimulateError()) {
            throw new IllegalStateException("Fallo simulado en auditoría de orden #" + event.getOrderId());
        }

        log.info("[AUDIT-DOMAIN] Trazabilidad registrada -> Orden #{}: {} ({} -> {}) por '{}' [partition={}, offset={}]",
                event.getOrderId(),
                event.getEventType(),
                event.getEstadoAnterior(),
                event.getEstadoNuevo(),
                event.getUsuario(),
                partition,
                offset);
    }

    public void auditCatalogMetric(AnalyticsMetricEventDTO metric, int partition, long offset) {
        if (metric == null || metric.getCategory() == null) {
            throw new IllegalArgumentException("Métrica nula o sin categoría");
        }

        log.info("[AUDIT-DOMAIN] Cambio en catálogo/métrica registrado -> Origen={}, Categoría={}, Entidad='{}', Detalle='{}' [partition={}, offset={}]",
                metric.getSourceService(),
                metric.getCategory(),
                metric.getEntityName(),
                metric.getDetails(),
                partition,
                offset);
    }
}
