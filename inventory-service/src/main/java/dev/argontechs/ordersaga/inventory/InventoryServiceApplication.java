package dev.argontechs.ordersaga.inventory;

import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.boot.autoconfigure.domain.EntityScan;
import org.springframework.data.jpa.repository.config.EnableJpaRepositories;
import org.springframework.scheduling.annotation.EnableScheduling;

@SpringBootApplication(scanBasePackages = "dev.argontechs.ordersaga")
@EntityScan(basePackages = "dev.argontechs.ordersaga")
@EnableJpaRepositories(basePackages = "dev.argontechs.ordersaga")
@EnableScheduling
public class InventoryServiceApplication {
    public static void main(String[] args) {
        SpringApplication.run(InventoryServiceApplication.class, args);
    }
}
