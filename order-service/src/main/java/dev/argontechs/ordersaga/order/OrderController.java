package dev.argontechs.ordersaga.order;

import jakarta.validation.Valid;
import jakarta.validation.constraints.NotBlank;
import jakarta.validation.constraints.NotEmpty;
import org.springframework.http.HttpStatus;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.*;

import java.math.BigDecimal;
import java.util.List;
import java.util.Map;
import java.util.UUID;

@RestController
@RequestMapping("/orders")
public class OrderController {

    private final OrderService service;
    private final OrderRepository orders;

    public OrderController(OrderService service, OrderRepository orders) {
        this.service = service;
        this.orders = orders;
    }

    record ItemDto(String productId, int quantity, BigDecimal unitPrice) {}
    record CreateOrderRequest(@NotBlank String customerId, @NotEmpty List<ItemDto> items) {}

    @PostMapping
    public ResponseEntity<Map<String, Object>> create(@Valid @RequestBody CreateOrderRequest req) {
        var orderId = service.createOrder(req.customerId(), req.items());
        return ResponseEntity.status(HttpStatus.CREATED).body(Map.of("orderId", orderId.toString()));
    }

    @GetMapping("/{id}")
    public ResponseEntity<Map<String, Object>> get(@PathVariable UUID id) {
        return orders.findById(id)
                .<ResponseEntity<Map<String, Object>>>map(o -> {
                    var body = new java.util.HashMap<String, Object>();
                    body.put("orderId", o.getId().toString());
                    body.put("status", o.getStatus().name());
                    body.put("cancellationReason", o.getCancellationReason());
                    return ResponseEntity.ok(body);
                })
                .orElse(ResponseEntity.notFound().build());
    }
}
