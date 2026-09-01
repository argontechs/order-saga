package dev.argontechs.ordersaga.order;

import dev.argontechs.ordersaga.events.OrderItem;
import jakarta.validation.Valid;
import jakarta.validation.constraints.NotBlank;
import jakarta.validation.constraints.NotEmpty;
import org.springframework.http.HttpStatus;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.*;

import java.util.List;
import java.util.Map;

@RestController
@RequestMapping("/orders")
public class OrderController {

    private final OrderService service;

    public OrderController(OrderService service) {
        this.service = service;
    }

    record CreateOrderRequest(@NotBlank String customerId, @NotEmpty List<OrderItem> items) {}

    @PostMapping
    public ResponseEntity<Map<String, Object>> create(@Valid @RequestBody CreateOrderRequest req) {
        var orderId = service.createOrder(req.customerId(), req.items());
        return ResponseEntity.status(HttpStatus.CREATED).body(Map.of("orderId", orderId.toString()));
    }
}
