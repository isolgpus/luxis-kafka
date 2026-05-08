package org.example;

import io.kiw.luxis.web.messaging.OutboxEvent;
import io.kiw.luxis.web.messaging.PendingOutboxEvent;
import io.kiw.luxis.web.messaging.Publisher;
import io.vertx.core.Future;
import io.vertx.core.Vertx;
import io.vertx.kafka.client.producer.KafkaProducer;
import io.vertx.kafka.client.producer.KafkaProducerRecord;

import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

public final class KafkaPublisher implements Publisher, AutoCloseable {

    private final KafkaProducer<String, byte[]> producer;

    public KafkaPublisher(final Vertx vertx, final Map<String, String> config) {
        final Map<String, String> merged = new java.util.HashMap<>(config);
        merged.putIfAbsent("key.serializer", "org.apache.kafka.common.serialization.StringSerializer");
        merged.putIfAbsent("value.serializer", "org.apache.kafka.common.serialization.ByteArraySerializer");
        merged.putIfAbsent("enable.idempotence", "true");
        merged.putIfAbsent("acks", "all");
        this.producer = KafkaProducer.create(vertx, merged);
    }

    @Override
    public Future<Void> publish(final List<PendingOutboxEvent> events) {
        if (events.isEmpty()) {
            return Future.succeededFuture();
        }
        final List<Future<?>> sends = new ArrayList<>(events.size());
        for (final PendingOutboxEvent event : events) {
            sends.add(producer.send(toRecord(event)));
        }
        return Future.all(sends).mapEmpty();
    }

    private static KafkaProducerRecord<String, byte[]> toRecord(final PendingOutboxEvent event) {
        final byte[] payload = switch (event.event().payload()) {
            case OutboxEvent.Payload.Str s -> s.value().getBytes(StandardCharsets.UTF_8);
            case OutboxEvent.Payload.Bytes b -> b.value();
            case OutboxEvent.Payload.Buf b -> readBuf(b.value());
        };
        return KafkaProducerRecord.<String, byte[]>create(event.event().key(), null, payload)
                .addHeader("x-luxis-event-id", Long.toString(event.id()));
    }

    private static byte[] readBuf(final ByteBuffer buf) {
        final byte[] out = new byte[buf.remaining()];
        buf.duplicate().get(out);
        return out;
    }

    @Override
    public void close() {
        producer.close();
    }
}
