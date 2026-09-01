package dev.argontechs.ordersaga.inventory;

import dev.argontechs.ordersaga.events.*;
import dev.argontechs.ordersaga.messaging.OutboxWriter;
import jakarta.persistence.EntityManager;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Transactional;

import java.time.Instant;
import java.util.Comparator;
import java.util.List;
import java.util.UUID;

@Service
public class InventoryService {

    private final EntityManager em;
    private final OutboxWriter outbox;

    public InventoryService(EntityManager em, OutboxWriter outbox) {
        this.em = em;
        this.outbox = outbox;
    }

    /** Lock rows in productId order (deadlock avoidance), verify ALL, then apply — so a
     *  shortage leaves stock untouched and only an OutOfStock event committed. */
    @Transactional
    public void reserve(UUID orderId, List<OrderItem> items) {
        var sorted = items.stream().sorted(Comparator.comparing(OrderItem::getProductId)).toList();
        for (var item : sorted) {
            var available = ((Number) em.createNativeQuery(
                    "SELECT available FROM stock WHERE product_id = ?1 FOR UPDATE")
                    .setParameter(1, item.getProductId()).getSingleResult()).intValue();
            if (available < item.getQuantity()) {
                outbox.write(Topics.INVENTORY, orderId,
                        new OutOfStock(UUID.randomUUID(), orderId, Instant.now(), item.getProductId()));
                return;
            }
        }
        for (var item : sorted) {
            em.createNativeQuery("UPDATE stock SET available = available - ?1 WHERE product_id = ?2")
                    .setParameter(1, item.getQuantity()).setParameter(2, item.getProductId()).executeUpdate();
            em.createNativeQuery("INSERT INTO reservations (order_id, product_id, quantity) VALUES (?1, ?2, ?3)")
                    .setParameter(1, orderId).setParameter(2, item.getProductId())
                    .setParameter(3, item.getQuantity()).executeUpdate();
        }
        outbox.write(Topics.INVENTORY, orderId,
                new InventoryReserved(UUID.randomUUID(), orderId, Instant.now()));
    }

    /** Compensation: restore reserved quantities. Idempotent — reservations are deleted as they restore. */
    @Transactional
    public void release(UUID orderId) {
        var rows = em.createNativeQuery(
                "DELETE FROM reservations WHERE order_id = ?1 RETURNING product_id, quantity")
                .setParameter(1, orderId).getResultList();
        if (rows.isEmpty()) return;
        for (var rowObj : rows) {
            var row = (Object[]) rowObj;
            em.createNativeQuery("UPDATE stock SET available = available + ?1 WHERE product_id = ?2")
                    .setParameter(1, ((Number) row[1]).intValue()).setParameter(2, row[0]).executeUpdate();
        }
        outbox.write(Topics.INVENTORY, orderId,
                new InventoryReleased(UUID.randomUUID(), orderId, Instant.now()));
    }
}
