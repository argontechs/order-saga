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
    public UUID createOrder(String customerId, List<OrderItem> items) {
        validateItems(items);

        var orderId = UUID.randomUUID();
        var total = items.stream()
                .map(i -> i.unitPrice().multiply(BigDecimal.valueOf(i.quantity())))
                .reduce(BigDecimal.ZERO, BigDecimal::add);

        orders.save(new Order(orderId, customerId, total));
        for (var item : items) {
            em.createNativeQuery("INSERT INTO order_items (order_id, product_id, quantity, unit_price) VALUES (?,?,?,?)")
              .setParameter(1, orderId).setParameter(2, item.productId())
              .setParameter(3, item.quantity()).setParameter(4, item.unitPrice())
              .executeUpdate();
        }
        outbox.write(Topics.ORDERS, orderId,
                new OrderCreated(UUID.randomUUID(), orderId, Instant.now(), customerId, items, total));
        return orderId;
    }

    // Guards against inputs that would otherwise sail through bean validation and dead-end the
    // saga downstream: a duplicate productId collides with inventory's (order_id, product_id)
    // PK on the second reservation insert (retries -> DLT, order stuck PAID forever with no
    // compensation); a non-positive quantity passes inventory's `available < qty` check and can
    // *increase* stock via `available - (negative qty)`.
    private void validateItems(List<OrderItem> items) {
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
