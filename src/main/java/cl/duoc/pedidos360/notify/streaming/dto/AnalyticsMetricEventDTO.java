package cl.duoc.pedidos360.notify.streaming.dto;

import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.NoArgsConstructor;

import java.math.BigDecimal;
import java.time.Instant;

@Data
@Builder
@NoArgsConstructor
@AllArgsConstructor
public class AnalyticsMetricEventDTO {
    private String metricId;
    private String sourceService;
    private String category;
    private Long entityId;
    private String entityName;
    private BigDecimal amount;
    private Integer quantityDelta;
    private String details;
    private Instant timestamp;
}
