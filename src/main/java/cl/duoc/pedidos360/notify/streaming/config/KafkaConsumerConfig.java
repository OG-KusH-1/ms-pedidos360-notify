package cl.duoc.pedidos360.notify.streaming.config;

import cl.duoc.pedidos360.notify.streaming.dto.AnalyticsMetricEventDTO;
import cl.duoc.pedidos360.notify.streaming.dto.OrderStreamEventDTO;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.kafka.config.ConcurrentKafkaListenerContainerFactory;
import org.springframework.kafka.core.DefaultKafkaConsumerFactory;
import org.springframework.kafka.listener.ContainerProperties;
import org.springframework.kafka.listener.DefaultErrorHandler;
import org.springframework.kafka.support.serializer.ErrorHandlingDeserializer;
import org.springframework.kafka.support.serializer.JsonDeserializer;
import org.springframework.util.backoff.FixedBackOff;

import java.util.HashMap;
import java.util.Map;

/**
 * Configuración de consumidores Kafka en ms-pedidos360-notify.
 * Emplea Consumer Group 'cg-audit-logger' con concurrencia 3 (balanceo entre 3 particiones)
 * y política de reintentos controlada sin afectar al resto del flujo.
 */
@Configuration
@Slf4j
public class KafkaConsumerConfig {

    @Value("${spring.kafka.bootstrap-servers}")
    private String bootstrapServers;

    private Map<String, Object> consumerProps(String groupId, Class<?> targetType) {
        Map<String, Object> props = new HashMap<>();
        props.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers);
        props.put(ConsumerConfig.GROUP_ID_CONFIG, groupId);
        props.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
        props.put(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, false);
        props.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, ErrorHandlingDeserializer.class);
        props.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, ErrorHandlingDeserializer.class);
        props.put(ErrorHandlingDeserializer.KEY_DESERIALIZER_CLASS, StringDeserializer.class);
        props.put(ErrorHandlingDeserializer.VALUE_DESERIALIZER_CLASS, JsonDeserializer.class);
        props.put(JsonDeserializer.VALUE_DEFAULT_TYPE, targetType.getName());
        props.put(JsonDeserializer.TRUSTED_PACKAGES, "*");
        props.put(JsonDeserializer.USE_TYPE_INFO_HEADERS, false);
        return props;
    }

    @Bean
    public DefaultErrorHandler notifyKafkaErrorHandler() {
        FixedBackOff backOff = new FixedBackOff(1000L, 2L);
        DefaultErrorHandler handler = new DefaultErrorHandler((record, ex) ->
                log.error("[NOTIFY-KAFKA-ERROR] Mensaje descartado tras reintentos en topic='{}', partition={}, offset={}, error='{}'",
                        record.topic(), record.partition(), record.offset(), ex.getMessage()),
                backOff);
        handler.addNotRetryableExceptions(IllegalArgumentException.class);
        return handler;
    }

    @Bean
    public ConcurrentKafkaListenerContainerFactory<String, OrderStreamEventDTO> auditOrderKafkaFactory(
            DefaultErrorHandler notifyKafkaErrorHandler) {
        ConcurrentKafkaListenerContainerFactory<String, OrderStreamEventDTO> factory =
                new ConcurrentKafkaListenerContainerFactory<>();
        factory.setConsumerFactory(new DefaultKafkaConsumerFactory<>(consumerProps("cg-audit-logger", OrderStreamEventDTO.class)));
        factory.setConcurrency(3);
        factory.getContainerProperties().setAckMode(ContainerProperties.AckMode.MANUAL_IMMEDIATE);
        factory.setCommonErrorHandler(notifyKafkaErrorHandler);
        return factory;
    }

    @Bean
    public ConcurrentKafkaListenerContainerFactory<String, AnalyticsMetricEventDTO> auditMetricKafkaFactory(
            DefaultErrorHandler notifyKafkaErrorHandler) {
        ConcurrentKafkaListenerContainerFactory<String, AnalyticsMetricEventDTO> factory =
                new ConcurrentKafkaListenerContainerFactory<>();
        factory.setConsumerFactory(new DefaultKafkaConsumerFactory<>(consumerProps("cg-audit-logger", AnalyticsMetricEventDTO.class)));
        factory.setConcurrency(3);
        factory.getContainerProperties().setAckMode(ContainerProperties.AckMode.MANUAL_IMMEDIATE);
        factory.setCommonErrorHandler(notifyKafkaErrorHandler);
        return factory;
    }
}
