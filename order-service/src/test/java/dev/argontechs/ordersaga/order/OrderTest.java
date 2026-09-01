package dev.argontechs.ordersaga.order;

import org.junit.jupiter.api.Test;

import java.math.BigDecimal;
import java.util.UUID;

import static org.assertj.core.api.Assertions.assertThat;

class OrderTest {

    @Test
    void advanceToCancelledIsANoOp() {
        var order = new Order(UUID.randomUUID(), "cust-1", BigDecimal.TEN);

        order.advanceTo(OrderStatus.CANCELLED);

        assertThat(order.getStatus()).isEqualTo(OrderStatus.PENDING);
        assertThat(order.getCancellationReason()).isNull();
    }

    @Test
    void cancelSetsStatusAndReason() {
        var order = new Order(UUID.randomUUID(), "cust-1", BigDecimal.TEN);

        order.cancel("out of stock");

        assertThat(order.getStatus()).isEqualTo(OrderStatus.CANCELLED);
        assertThat(order.getCancellationReason()).isEqualTo("out of stock");
    }

    @Test
    void advanceToAfterCancelIsANoOp() {
        var order = new Order(UUID.randomUUID(), "cust-1", BigDecimal.TEN);
        order.cancel("out of stock");

        order.advanceTo(OrderStatus.PAID);

        assertThat(order.getStatus()).isEqualTo(OrderStatus.CANCELLED);
        assertThat(order.getCancellationReason()).isEqualTo("out of stock");
    }
}
