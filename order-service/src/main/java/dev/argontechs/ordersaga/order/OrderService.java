package dev.argontechs.ordersaga.order;

import dev.argontechs.ordersaga.events.OrderCreated;
import dev.argontechs.ordersaga.events.OrderItem;
import dev.argontechs.ordersaga.events.Topics;
import dev.argontechs.ordersaga.messaging.OutboxWriter;
import jakarta.persistence.EntityManager;
import org.springframework.http.HttpStatus;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Transactional;
import org.springframework.web.server.ResponseStatusException;

import java.math.BigDecimal;
import java.math.RoundingMode;
import java.time.Instant;
import java.util.HashSet;
import java.util.List;
import java.util.UUID;

@Service
public class OrderService {

    private final OrderRepository orders;
    private final OutboxWriter outbox;
    private final EntityManager em;

    public OrderService(OrderRepository orders, OutboxWriter outbox, EntityManager em) {
        this.orders = orders;
        this.outbox = outbox;
        this.em = em;
    }

    @Transactional
    public UUID createOrder(String customerId, List<OrderController.ItemDto> items) {
        validateItems(items);

        var orderItems = items.stream()
                .map(dto -> new OrderItem(dto.productId(), dto.quantity(), dto.unitPrice().setScale(2, RoundingMode.HALF_UP)))
                .toList();

        var orderId = UUID.randomUUID();
        var total = orderItems.stream()
                .map(i -> i.getUnitPrice().multiply(BigDecimal.valueOf(i.getQuantity())))
                .reduce(BigDecimal.ZERO, BigDecimal::add);

        orders.save(new Order(orderId, customerId, total));
        for (var item : orderItems) {
            em.createNativeQuery("INSERT INTO order_items (order_id, product_id, quantity, unit_price) VALUES (?,?,?,?)")
              .setParameter(1, orderId).setParameter(2, item.getProductId())
              .setParameter(3, item.getQuantity()).setParameter(4, item.getUnitPrice())
              .executeUpdate();
        }
        outbox.write(Topics.ORDERS, orderId,
                new OrderCreated(UUID.randomUUID(), orderId, Instant.now(), customerId, orderItems, total));
        return orderId;
    }

    // Guards against inputs that would otherwise sail through bean validation and dead-end the
    // saga downstream: a duplicate productId collides with inventory's (order_id, product_id)
    // PK on the second reservation insert (retries -> DLT, order stuck PAID forever with no
    // compensation); a non-positive quantity passes inventory's `available < qty` check and can
    // *increase* stock via `available - (negative qty)`.
    private void validateItems(List<OrderController.ItemDto> items) {
        var seenProductIds = new HashSet<String>();
        for (var item : items) {
            if (!seenProductIds.add(item.productId())) {
                throw new ResponseStatusException(HttpStatus.BAD_REQUEST,
                        "duplicate productId in order items: " + item.productId());
            }
            if (item.quantity() < 1) {
                throw new ResponseStatusException(HttpStatus.BAD_REQUEST,
                        "quantity must be >= 1 for productId: " + item.productId());
            }
            if (item.unitPrice() == null || item.unitPrice().signum() <= 0) {
                throw new ResponseStatusException(HttpStatus.BAD_REQUEST,
                        "unitPrice must be > 0 for productId: " + item.productId());
            }
        }
    }
}
