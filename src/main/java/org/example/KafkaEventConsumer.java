package org.example;

import io.kiw.luxis.web.messaging.EventConsumer;
import io.kiw.luxis.web.messaging.EventDispatcher;
import io.vertx.core.Vertx;
import io.vertx.kafka.client.consumer.KafkaConsumer;
import tools.jackson.databind.JsonNode;
import tools.jackson.databind.ObjectMapper;

import java.nio.ByteBuffer;
import java.util.HashMap;
import java.util.Map;

public final class KafkaEventConsumer implements EventConsumer {

    private static final ObjectMapper MAPPER = new ObjectMapper();

    private final KafkaConsumer<String, byte[]> consumer;
    private final String topic;

    public KafkaEventConsumer(final Vertx vertx, final Map<String, String> config, final String topic) {
        final Map<String, String> merged = new HashMap<>(config);
        merged.putIfAbsent("key.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
        merged.putIfAbsent("value.deserializer", "org.apache.kafka.common.serialization.ByteArrayDeserializer");
        merged.putIfAbsent("auto.offset.reset", "earliest");
        merged.putIfAbsent("enable.auto.commit", "false");
        this.consumer = KafkaConsumer.create(vertx, merged);
        this.topic = topic;
    }

    @Override
    public String topic() {
        return topic;
    }

    @Override
    public String extractKey(final ByteBuffer message) {
        final JsonNode node = MAPPER.readTree(toBytes(message));
        final JsonNode keyNode = node.get("key");
        if (keyNode == null || keyNode.isNull()) {
            throw new IllegalStateException("event envelope missing 'key' field");
        }
        return keyNode.asString();
    }

    @Override
    public <T> T decode(final ByteBuffer message, final Class<T> type) {
        final JsonNode node = MAPPER.readTree(toBytes(message));
        final JsonNode payload = node.get("payload");
        if (payload == null) {
            throw new IllegalStateException("event envelope missing 'payload' field");
        }
        return MAPPER.treeToValue(payload, type);
    }

    @Override
    public void start(final EventDispatcher dispatcher) {
        consumer.handler(record -> dispatcher.dispatch(ByteBuffer.wrap(record.value())));
        consumer.subscribe(topic);
    }

    @Override
    public void close() {
        consumer.close();
    }

    private static byte[] toBytes(final ByteBuffer buf) {
        final byte[] out = new byte[buf.remaining()];
        buf.duplicate().get(out);
        return out;
    }
}