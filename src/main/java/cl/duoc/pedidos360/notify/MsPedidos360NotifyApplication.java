package cl.duoc.pedidos360.notify;

import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;

/**
 * MsPedidos360NotifyApplication - Aplicación principal del microservicio de Notificaciones y RabbitMQ.
 */
@SpringBootApplication
public class MsPedidos360NotifyApplication {

    public static void main(String[] args) {
        SpringApplication.run(MsPedidos360NotifyApplication.class, args);
    }
}
