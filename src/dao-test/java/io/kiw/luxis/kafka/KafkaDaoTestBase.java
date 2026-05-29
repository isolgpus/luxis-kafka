package io.kiw.luxis.kafka;

import io.vertx.core.Vertx;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.slf4j.LoggerFactory;
import org.testcontainers.containers.KafkaContainer;
import org.testcontainers.containers.output.Slf4jLogConsumer;
import org.testcontainers.utility.DockerImageName;

import java.util.concurrent.TimeUnit;

public class KafkaDaoTestBase {

    private static final KafkaContainer KAFKA = new KafkaContainer(
            DockerImageName.parse("confluentinc/cp-kafka:7.6.0"))
            .withLogConsumer(new Slf4jLogConsumer(LoggerFactory.getLogger("kafka-container")));

    private static Vertx vertx;

    @BeforeClass
    public static void startKafka() {
        KAFKA.start();
        vertx = Vertx.vertx();
    }

    @AfterClass
    public static void stopKafka() throws Exception {
        if (vertx != null) {
            vertx.close().toCompletionStage().toCompletableFuture().get(5, TimeUnit.SECONDS);
        }
        KAFKA.stop();
    }

    protected static String bootstrapServers() {
        return KAFKA.getBootstrapServers();
    }

    protected static Vertx vertx() {
        return vertx;
    }
}
