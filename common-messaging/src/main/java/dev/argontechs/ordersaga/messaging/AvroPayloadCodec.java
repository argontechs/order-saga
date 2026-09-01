package dev.argontechs.ordersaga.messaging;

import org.apache.avro.io.DecoderFactory;
import org.apache.avro.io.EncoderFactory;
import org.apache.avro.specific.SpecificData;
import org.apache.avro.specific.SpecificDatumReader;
import org.apache.avro.specific.SpecificDatumWriter;
import org.apache.avro.specific.SpecificRecordBase;
import org.springframework.stereotype.Component;

import java.io.ByteArrayOutputStream;
import java.nio.charset.StandardCharsets;

/** Outbox payload codec: Avro JSON encoding, schema-faithful and human-readable in the DB.
 *  The outbox stores (type FQN, json); the publisher reconstructs the SpecificRecord and
 *  hands it to KafkaAvroSerializer — so the DB write never depends on registry availability. */
@Component
public class AvroPayloadCodec {

    // IMPORTANT: use the generated class's own SpecificData model — it carries the
    // logical-type conversions (BigDecimal/UUID/Instant). The default SpecificData.get()
    // does not, and would throw ClassCastException on the first decimal field.
    public String toJson(SpecificRecordBase event) {
        try {
            var out = new ByteArrayOutputStream();
            var writer = new SpecificDatumWriter<SpecificRecordBase>(event.getSchema(), event.getSpecificData());
            var encoder = EncoderFactory.get().jsonEncoder(event.getSchema(), out);
            writer.write(event, encoder);
            encoder.flush();
            return out.toString(StandardCharsets.UTF_8);
        } catch (Exception e) {
            throw new IllegalStateException("Avro JSON encode failed for " + event.getClass().getName(), e);
        }
    }

    public SpecificRecordBase fromJson(String type, String payload) {
        try {
            var clazz = Class.forName(type);
            var model = SpecificData.getForClass(clazz);
            var schema = model.getSchema(clazz);
            var reader = new SpecificDatumReader<SpecificRecordBase>(schema, schema, model);
            return reader.read(null, DecoderFactory.get().jsonDecoder(schema, payload));
        } catch (Exception e) {
            throw new IllegalStateException("Avro JSON decode failed for " + type, e);
        }
    }
}
