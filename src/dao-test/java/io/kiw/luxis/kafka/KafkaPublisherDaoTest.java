package io.kiw.luxis.kafka;

import io.kiw.luxis.result.Result;
import io.kiw.luxis.web.Luxis;
import io.kiw.luxis.web.WebServiceConfigBuilder;
import io.kiw.luxis.web.handler.JsonHandler;
import io.kiw.luxis.web.http.ErrorMessageResponse;
import io.kiw.luxis.web.http.ErrorStatusCode;
import io.kiw.luxis.web.http.HttpErrorResponse;
import io.kiw.luxis.web.http.HttpResult;
import io.kiw.luxis.web.http.Method;
import io.kiw.luxis.web.http.client.LuxisAsync;
import io.kiw.luxis.web.internal.LuxisPipeline;
import io.kiw.luxis.web.messaging.EventPlatform;
import io.kiw.luxis.web.pipeline.HttpStream;
import io.kiw.luxis.web.test.StubNetwork;
import io.kiw.luxis.web.test.StubRequest;
import io.kiw.luxis.web.test.StubTestClient;
import io.kiw.luxis.web.test.TestClient;
import io.kiw.luxis.web.test.TestHttpResponse;
import io.kiw.luxis.web.test.TestLuxis;
import io.kiw.luxis.web.test.VertxTestClient;
import io.vertx.core.Vertx;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.serialization.ByteArrayDeserializer;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.TimeUnit;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

public class KafkaPublisherDaoTest extends KafkaDaoTestBase {

    private final InMemoryDatabaseClient databaseClient = new InMemoryDatabaseClient();
    private final InMemoryOutboxStore outboxStore = new InMemoryOutboxStore();
    private KafkaPublisher publisher;
    private KafkaEventConsumer eventConsumer;
    private TestClient client;
    private final CompletableFuture<Ping> receivedPing = new CompletableFuture<>();
    private Luxis<Object> luxis;

    @Before
    public void setUp() {
        final Map<String, String> producerConfig = new HashMap<>();
        producerConfig.put("bootstrap.servers", bootstrapServers());
        publisher = new KafkaPublisher(vertx(), producerConfig);

        final Map<String, String> consumerConfig = new HashMap<>();
        consumerConfig.put("bootstrap.servers", bootstrapServers());
        consumerConfig.put("group.id", "luxis-event-consumer-" + UUID.randomUUID());
        eventConsumer = new KafkaEventConsumer(vertx(), consumerConfig, "luxis-events-" + UUID.randomUUID());

        luxis = Luxis.app(routes -> {
                    routes.jsonRoute("/publish-tx", Method.POST, null, PublishRequest.class, new PublishInTxHandler());
                    routes.jsonRoute("/publish-tx/rollback", Method.POST, null, PublishRequest.class, new PublishInTxRollbackHandler());
                    routes.jsonRoute("/publish-tx/many", Method.POST, null, PublishManyRequest.class, new PublishManyInTxHandler());
                    routes.jsonRoute("/publish-immediate", Method.POST, null, PublishRequest.class, new PublishImmediateHandler());

                    routes.eventRoute("ping", null, Ping.class, stream -> stream
                            .peek(ctx -> receivedPing.complete(ctx.in()))
                            .completeWithNoResponse());
                    return null;
                })
                .withDatabase(databaseClient)
                .withConfig(new WebServiceConfigBuilder().setPort(8080).build())
                .withEventPlatform(EventPlatform.of(publisher, outboxStore, eventConsumer)).start(Vertx.vertx());
        client = new VertxTestClient("localhost", 8080);

    }

    @After
    public void tearDown() throws Exception {
        if (publisher != null) {
            publisher.close();
        }
        if (luxis != null) {
            luxis.close();
        }
    }

    @Test
    public void transactionalPublishLandsOnKafka() throws Exception {
        final String topic = newTopic();
        final TestHttpResponse response = client.post(StubRequest.request("/publish-tx")
                .body("{\"topic\":\"" + topic + "\",\"message\":\"hello-tx\"}"));

        assertEquals(response.responseBody, 200, response.statusCode);

        final List<ConsumerRecord<String, byte[]>> records = consumeAtLeast(topic, 1, Duration.ofSeconds(15));
        assertEquals(1, records.size());
        assertEquals("hello-tx", new String(records.get(0).value(), StandardCharsets.UTF_8));
        client.assertNoMoreExceptions();
    }

    @Test
    public void rolledBackTransactionPublishesNothing() throws Exception {
        final String topic = newTopic();
        final TestHttpResponse response = client.post(StubRequest.request("/publish-tx/rollback")
                .body("{\"topic\":\"" + topic + "\",\"message\":\"should-not-arrive\"}"));

        assertEquals(500, response.statusCode);

        final List<ConsumerRecord<String, byte[]>> records = consumeFor(topic, Duration.ofSeconds(2));
        assertEquals("expected no events on a rolled-back transaction, got " + records.size(),
                0, records.size());
    }

    @Test
    public void batchOfEventsInOneTransactionAllLand() throws Exception {
        final String topic = newTopic();
        final TestHttpResponse response = client.post(StubRequest.request("/publish-tx/many")
                .body("{\"topic\":\"" + topic + "\",\"messages\":[\"a\",\"b\",\"c\"]}"));

        assertEquals(response.responseBody, 200, response.statusCode);

        final List<ConsumerRecord<String, byte[]>> records = consumeAtLeast(topic, 3, Duration.ofSeconds(15));
        final List<String> values = new ArrayList<>();
        for (final ConsumerRecord<String, byte[]> r : records) {
            values.add(new String(r.value(), StandardCharsets.UTF_8));
        }
        assertEquals(List.of("a", "b", "c"), values);
        assertTrue("luxis-event-id header should be stamped",
                records.get(0).headers().lastHeader("x-luxis-event-id") != null);
        client.assertNoMoreExceptions();
    }

    @Test
    public void publishedEventIsReceivedByEventConsumer() throws Exception {
        final String topic = eventConsumer.topic();
        final String envelope = "{\\\"key\\\":\\\"ping\\\",\\\"payload\\\":{\\\"text\\\":\\\"hello-event\\\"}}";
        final TestHttpResponse response = client.post(StubRequest.request("/publish-immediate")
                .body("{\"topic\":\"" + topic + "\",\"message\":\"" + envelope + "\"}"));

        assertEquals(response.responseBody, 200, response.statusCode);

        final Ping received = receivedPing.get(20, TimeUnit.SECONDS);
        assertEquals("hello-event", received.text);
        client.assertNoMoreExceptions();
    }

    @Test
    public void immediatePublishLandsOnKafka() throws Exception {
        final String topic = newTopic();
        final TestHttpResponse response = client.post(StubRequest.request("/publish-immediate")
                .body("{\"topic\":\"" + topic + "\",\"message\":\"hello-immediate\"}"));

        assertEquals(response.responseBody, 200, response.statusCode);

        final List<ConsumerRecord<String, byte[]>> records = consumeAtLeast(topic, 1, Duration.ofSeconds(15));
        assertEquals(1, records.size());
        assertEquals("hello-immediate", new String(records.get(0).value(), StandardCharsets.UTF_8));
        client.assertNoMoreExceptions();
    }

    private static String newTopic() {
        return "luxis-kafka-dao-" + UUID.randomUUID();
    }

    private List<ConsumerRecord<String, byte[]>> consumeAtLeast(final String topic, final int min, final Duration timeout) {
        try (KafkaConsumer<String, byte[]> consumer = newConsumer()) {
            consumer.subscribe(Collections.singletonList(topic));
            final long deadline = System.nanoTime() + timeout.toNanos();
            final List<ConsumerRecord<String, byte[]>> all = new ArrayList<>();
            while (System.nanoTime() < deadline) {
                final ConsumerRecords<String, byte[]> records = consumer.poll(Duration.ofMillis(500));
                for (final ConsumerRecord<String, byte[]> r : records) {
                    all.add(r);
                }
                if (all.size() >= min) {
                    return all;
                }
            }
            fail("timed out waiting for " + min + " records on " + topic + " (got " + all.size() + ")");
            return all;
        }
    }

    private List<ConsumerRecord<String, byte[]>> consumeFor(final String topic, final Duration window) {
        try (KafkaConsumer<String, byte[]> consumer = newConsumer()) {
            consumer.subscribe(Collections.singletonList(topic));
            final long deadline = System.nanoTime() + window.toNanos();
            final List<ConsumerRecord<String, byte[]>> all = new ArrayList<>();
            while (System.nanoTime() < deadline) {
                final ConsumerRecords<String, byte[]> records = consumer.poll(Duration.ofMillis(200));
                for (final ConsumerRecord<String, byte[]> r : records) {
                    all.add(r);
                }
            }
            return all;
        }
    }

    private KafkaConsumer<String, byte[]> newConsumer() {
        final Properties props = new Properties();
        props.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers());
        props.put(ConsumerConfig.GROUP_ID_CONFIG, "dao-test-" + UUID.randomUUID());
        props.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
        props.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class);
        props.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, ByteArrayDeserializer.class);
        return new KafkaConsumer<>(props);
    }

    public static final class PublishRequest {
        public String topic;
        public String message;
    }

    public static final class Ping {
        public String text;
    }

    public static final class PublishManyRequest {
        public String topic;
        public List<String> messages;
    }

    private static <T> LuxisAsync<T, HttpErrorResponse> succeeded(final T value) {
        return new LuxisAsync<>(CompletableFuture.completedFuture(Result.success(value)));
    }

    private static final class PublishInTxHandler implements JsonHandler<PublishRequest, PublishRequest, Object> {
        @Override
        public LuxisPipeline<PublishRequest> handle(final HttpStream<PublishRequest, Object> stream) {
            return stream
                    .inTransaction(tx -> tx
                            .asyncPeek(ctx -> {
                                ctx.publisher().publish(ctx.in().topic, ctx.in().message);
                                return succeeded(null);
                            })
                            .commit())
                    .complete(ctx -> HttpResult.success(ctx.in()));
        }
    }

    private static final class PublishInTxRollbackHandler implements JsonHandler<PublishRequest, PublishRequest, Object> {
        @Override
        public LuxisPipeline<PublishRequest> handle(final HttpStream<PublishRequest, Object> stream) {
            return stream
                    .inTransaction(tx -> tx
                            .asyncPeek(ctx -> {
                                ctx.publisher().publish(ctx.in().topic, ctx.in().message);
                                return succeeded(null);
                            })
                            .<PublishRequest>flatMap(ctx -> HttpResult.error(ErrorStatusCode.INTERNAL_SERVER_ERROR,
                                    new ErrorMessageResponse("forced rollback")))
                            .commit())
                    .complete(ctx -> HttpResult.success(ctx.in()));
        }
    }

    private static final class PublishManyInTxHandler implements JsonHandler<PublishManyRequest, PublishManyRequest, Object> {
        @Override
        public LuxisPipeline<PublishManyRequest> handle(final HttpStream<PublishManyRequest, Object> stream) {
            return stream
                    .inTransaction(tx -> tx
                            .asyncPeek(ctx -> {
                                for (final String msg : ctx.in().messages) {
                                    ctx.publisher().publish(ctx.in().topic, msg);
                                }
                                return succeeded(null);
                            })
                            .commit())
                    .complete(ctx -> HttpResult.success(ctx.in()));
        }
    }

    private static final class PublishImmediateHandler implements JsonHandler<PublishRequest, PublishRequest, Object> {
        @Override
        public LuxisPipeline<PublishRequest> handle(final HttpStream<PublishRequest, Object> stream) {
            return stream
                    .asyncPeek(ctx -> ctx.publisher().publish(ctx.in().topic, ctx.in().message))
                    .complete(ctx -> HttpResult.success(ctx.in()));
        }
    }
}
