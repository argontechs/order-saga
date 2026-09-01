package dev.argontechs.ordersaga.payment;

import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Component;

import java.math.BigDecimal;
import java.util.concurrent.ThreadLocalRandom;

/** Fake payment provider. Declines totals >= 10000.00 (deterministic, demo-able),
 *  plus a configurable random failure rate for chaos demos. */
@Component
public class FakePsp {

    static final BigDecimal DECLINE_THRESHOLD = new BigDecimal("10000.00");
    private final double failureRate;

    public FakePsp(@Value("${psp.failure-rate:0.0}") double failureRate) {
        this.failureRate = failureRate;
    }

    public boolean authorize(BigDecimal amount) {
        if (amount.compareTo(DECLINE_THRESHOLD) >= 0) return false;
        return ThreadLocalRandom.current().nextDouble() >= failureRate;
    }
}
