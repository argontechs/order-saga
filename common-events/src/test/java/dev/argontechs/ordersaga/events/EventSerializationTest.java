package dev.argontechs.ordersaga.events;

import org.apache.avro.io.*;
import org.apache.avro.specific.SpecificDatumReader;
import org.apache.avro.specific.SpecificDatumWriter;
import org.junit.jupiter.api.Test;

import java.io.ByteArrayOutputStream;
import java.math.BigDecimal;
import java.time.Instant;
import java.util.List;
import java.util.UUID;

import static org.assertj.core.api.Assertions.assertThat;

class EventSerializationTest {

    @SuppressWarnings("unchecked")
    private <T extends org.apache.avro.specific.SpecificRecordBase> T roundTrip(T record) throws Exception {
        // Use the record's own SpecificData model — it registers the logical-type
        // conversions (decimal/uuid/timestamp) that the default model lacks.
        var model = record.getSpecificData();
        var out = new ByteArrayOutputStream();
        var writer = new SpecificDatumWriter<T>(record.getSchema(), model);
        var encoder = EncoderFactory.get().binaryEncoder(out, null);
        writer.write(record, encoder);
        encoder.flush();
        var reader = new SpecificDatumReader<T>(record.getSchema(), record.getSchema(), model);
        return reader.read(null, DecoderFactory.get().binaryDecoder(out.toByteArray(), null));
    }

    @Test
    void orderCreatedRoundTrips() throws Exception {
        var event = new OrderCreated(UUID.randomUUID(), UUID.randomUUID(),
                Instant.now().truncatedTo(java.time.temporal.ChronoUnit.MILLIS), "cust-1",
                List.of(new OrderItem("P100", 2, new BigDecimal("49.90"))),
                new BigDecimal("99.80"));
        assertThat(roundTrip(event)).isEqualTo(event);
    }

    @Test
    void paymentAuthorizedRoundTrips() throws Exception {
        var event = new PaymentAuthorized(UUID.randomUUID(), UUID.randomUUID(),
                Instant.now().truncatedTo(java.time.temporal.ChronoUnit.MILLIS),
                UUID.randomUUID(), new BigDecimal("99.80"),
                List.of(new OrderItem("P100", 2, new BigDecimal("49.90"))));
        assertThat(roundTrip(event)).isEqualTo(event);
    }
}
