package dev.argontechs.ordersaga.order;

import jakarta.persistence.*;
import java.math.BigDecimal;
import java.time.Instant;
import java.util.UUID;

@Entity
@Table(name = "orders")
public class Order {
    @Id
    private UUID id;
    @Column(name = "customer_id", nullable = false)
    private String customerId;
    @Enumerated(EnumType.STRING)
    @Column(nullable = false)
    private OrderStatus status;
    @Column(name = "total_amount", nullable = false)
    private BigDecimal totalAmount;
    @Column(name = "cancellation_reason")
    private String cancellationReason;
    @Column(name = "created_at", nullable = false)
    private Instant createdAt = Instant.now();

    protected Order() {}

    public Order(UUID id, String customerId, BigDecimal totalAmount) {
        this.id = id;
        this.customerId = customerId;
        this.totalAmount = totalAmount;
        this.status = OrderStatus.PENDING;
    }

    public UUID getId() { return id; }
    public String getCustomerId() { return customerId; }
    public OrderStatus getStatus() { return status; }
    public BigDecimal getTotalAmount() { return totalAmount; }
    public String getCancellationReason() { return cancellationReason; }

    /** Advance only forward; ignore stale/out-of-order events. Terminal states never change. */
    public void advanceTo(OrderStatus next) {
        if (status == OrderStatus.CANCELLED || status == OrderStatus.CONFIRMED) return;
        if (next.ordinal() > status.ordinal()) this.status = next;
    }

    public void cancel(String reason) {
        if (status == OrderStatus.CONFIRMED || status == OrderStatus.CANCELLED) return;
        this.status = OrderStatus.CANCELLED;
        this.cancellationReason = reason;
    }
}
