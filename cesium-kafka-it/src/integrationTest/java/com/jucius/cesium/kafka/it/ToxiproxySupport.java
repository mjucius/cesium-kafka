package com.jucius.cesium.kafka.it;

import static org.awaitility.Awaitility.await;
import static org.junit.jupiter.api.Assertions.assertEquals;

import eu.rekawek.toxiproxy.model.ToxicDirection;
import java.io.IOException;
import java.io.UncheckedIOException;
import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.atomic.AtomicLong;
import org.apache.kafka.clients.admin.Admin;
import org.apache.kafka.clients.admin.AdminClientConfig;
import org.apache.kafka.clients.admin.NewTopic;
import org.apache.kafka.clients.consumer.Consumer;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.errors.UnknownTopicOrPartitionException;
import org.apache.kafka.common.serialization.ByteArrayDeserializer;
import org.apache.kafka.common.serialization.ByteArraySerializer;
import org.testcontainers.containers.Network;
import org.testcontainers.containers.ToxiproxyContainer;
import org.testcontainers.containers.ToxiproxyContainer.ContainerProxy;
import org.testcontainers.kafka.KafkaContainer;
import org.testcontainers.utility.DockerImageName;

/**
 * M6 fault-injection cluster: a dedicated KRaft broker fronted by a Toxiproxy so a client&#8596;broker
 * link can be cut, delayed, or black-holed deterministically (design R21, §11.3-3/4/5/10). This is
 * the shared infrastructure the M6 scenario suites build on; one cluster per JVM, lazily started
 * and never stopped (Testcontainers' Ryuk reaper removes it on JVM exit), exactly like
 * {@link KafkaIT#KAFKA}.
 *
 * <p><strong>Why a dedicated broker (option (b)).</strong> The KRaft + Toxiproxy wrinkle is that a
 * proxied client bootstraps through the proxy but the broker hands it back an
 * {@code advertised.listeners} address it must use for every subsequent connection — so for the
 * proxy to stay in the path that advertised address has to be the <em>host-visible proxy</em>
 * endpoint, not the broker's own mapped port. The shared {@link KafkaIT#KAFKA} singleton is already
 * started (with its own mapped-port advertisement) before any test runs and is depended on by 28
 * existing ITs, so it cannot be retrofitted with a proxied listener. This class therefore stands up
 * a second, purpose-built broker on a user-defined network with <em>two</em> advertised listeners:
 *
 * <ul>
 *   <li>a <strong>direct</strong> {@code PLAINTEXT} listener ({@link #directBootstrap()}) — the
 *       healthy path, immune to every toxic; used by the test's verifier admin/consumers and by any
 *       group member that must stay reachable while another is partitioned;
 *   <li>a <strong>proxied</strong> listener ({@link #proxiedBootstrap()}) whose advertised address
 *       is the Toxiproxy host:port — every byte of a member routed here (metadata, heartbeats,
 *       fetches, produces, transaction RPCs) flows through {@link #brokerProxy()}, so a single
 *       {@code cut} partitions exactly that member.
 * </ul>
 *
 * <p>Because a broker returns the advertised listener matching the listener a connection arrived on,
 * a member on {@link #directBootstrap()} and a member on {@link #proxiedBootstrap()} share one
 * broker and one consumer group yet have independent network fates — the property the zombie-fencing
 * and penalty-box scenarios need (partition ONE member, the other keeps its session alive).
 *
 * <p><strong>Single proxied listener (v1 scope).</strong> The new {@code KafkaContainer} stores its
 * custom listeners and advertised-address suppliers in {@link java.util.HashSet}s and pairs them by
 * positional index; with one custom listener the pairing is unambiguous, with several the
 * bind&#8596;advertised pairing is order-unstable. v1 therefore exposes exactly one proxied path
 * (plus the always-healthy direct path), which covers "one member partitioned while the rest stay
 * healthy". A test chooses <em>which</em> member is partitionable by routing it through
 * {@link #proxiedBootstrap()}.
 */
final class ToxiproxySupport {

    /** Matches {@link KafkaIT#KAFKA_IMAGE} — same broker the rest of the suite validates against. */
    static final DockerImageName KAFKA_IMAGE = DockerImageName.parse("apache/kafka:4.3.0");

    /** Toxiproxy image (GHCR mirror; the testcontainers module accepts both Docker Hub and GHCR). */
    static final DockerImageName TOXIPROXY_IMAGE = DockerImageName.parse("ghcr.io/shopify/toxiproxy:2.5.0")
            .asCompatibleSubstituteFor("ghcr.io/shopify/toxiproxy");

    /** In-network alias + port of the broker's proxied listener; Toxiproxy forwards here. */
    private static final String KAFKA_NETWORK_ALIAS = "cesium-m6-kafka";

    private static final int PROXIED_LISTENER_PORT = 19092;

    /** A short toxic name; cut/restore toggle the connection rather than add a named toxic. */
    private static final String LATENCY_TOXIC = "cesium-latency";

    private static final String TIMEOUT_TOXIC = "cesium-timeout";

    private static volatile ToxiproxySupport instance;

    private static final AtomicLong UNIQUE = new AtomicLong();

    private final ToxiproxyContainer toxiproxy;
    private final KafkaContainer kafka;
    private final Proxy brokerProxy;
    private final String directBootstrap;
    private final String proxiedBootstrap;
    private final Admin admin;

    // ToxiproxyContainer.ContainerProxy / getProxy are deprecated in favor of the raw toxiproxy-java
    // client, but remain the supported, fully-functional path in testcontainers 1.21.3 (setConnectionCut
    // + toxics() are what we need); the raw client is more ceremony for no behavioral gain.
    @SuppressWarnings("deprecation")
    private ToxiproxySupport() {
        Network network = Network.newNetwork();

        // 1. Toxiproxy first: its proxy ports are pre-exposed at construction, so the host-mapped
        //    address exists the moment it starts — before the broker needs to advertise it.
        this.toxiproxy = new ToxiproxyContainer(TOXIPROXY_IMAGE).withNetwork(network);
        toxiproxy.start();

        // 2. Define the proxy whose upstream is the broker's (not-yet-started) in-network listener.
        //    The upstream host is a network alias resolved lazily on first client connection.
        ContainerProxy proxy = toxiproxy.getProxy(KAFKA_NETWORK_ALIAS, PROXIED_LISTENER_PORT);
        this.brokerProxy = new Proxy(proxy);
        String advertisedProxyEndpoint = proxy.getContainerIpAddress() + ":" + proxy.getProxyPort();

        // 3. Broker with a direct PLAINTEXT listener (getBootstrapServers) AND a proxied listener
        //    advertised at the Toxiproxy host endpoint. withListener(host:port) registers the
        //    network alias and binds the listener; the supplier supplies its advertised address.
        this.kafka = new KafkaContainer(KAFKA_IMAGE)
                .withNetwork(network)
                .withNetworkAliases(KAFKA_NETWORK_ALIAS)
                .withListener(KAFKA_NETWORK_ALIAS + ":" + PROXIED_LISTENER_PORT, () -> advertisedProxyEndpoint)
                .withEnv("KAFKA_AUTO_CREATE_TOPICS_ENABLE", "false")
                // Let the multi-instance suites evict a killed static member in seconds.
                .withEnv("KAFKA_GROUP_MIN_SESSION_TIMEOUT_MS", "1000");
        kafka.start();

        this.directBootstrap = kafka.getBootstrapServers();
        this.proxiedBootstrap = advertisedProxyEndpoint;
        this.admin = Admin.create(Map.<String, Object>of(AdminClientConfig.BOOTSTRAP_SERVERS_CONFIG, directBootstrap));
    }

    /** The shared, lazily started cluster (one per JVM). */
    static synchronized ToxiproxySupport shared() {
        if (instance == null) {
            instance = new ToxiproxySupport();
        }
        return instance;
    }

    // ------------------------------------------------------------------ bootstraps & handles

    /** The healthy, never-proxied bootstrap; verifier admin/consumers and healthy members use it. */
    String directBootstrap() {
        return directBootstrap;
    }

    /** The bootstrap whose every connection flows through {@link #brokerProxy()} (partitionable). */
    String proxiedBootstrap() {
        return proxiedBootstrap;
    }

    /** The single proxy in front of the broker's proxied listener. */
    Proxy brokerProxy() {
        return brokerProxy;
    }

    /** A verifier {@link Admin} on the direct path — immune to whatever the proxy is doing. */
    Admin admin() {
        return admin;
    }

    // ------------------------------------------------------------------ topic / client helpers

    /** Unique per-cluster name so M6 suites never collide on topic/app-id. */
    static String unique(String prefix) {
        return prefix + "-" + UNIQUE.incrementAndGet() + "-"
                + UUID.randomUUID().toString().substring(0, 8);
    }

    /** Creates a uniquely named topic over the direct path and returns its name. */
    String createTopic(String prefix, int partitions) {
        String name = unique(prefix);
        NewTopic topic = new NewTopic(name, partitions, (short) 1);
        try {
            admin.createTopics(List.of(topic)).all().get(30, java.util.concurrent.TimeUnit.SECONDS);
        } catch (Exception e) {
            throw new AssertionError("createTopic " + name + " failed", e);
        }
        // Same reason as KafkaIT.createNamedTopic: the controller's ack does not mean a broker will
        // describe it yet, and this cluster's topics also go straight into an engine harness.
        await("topic " + name + " describable").atMost(KafkaIT.WAIT).until(() -> topicExists(name));
        return name;
    }

    /** True when this cluster will describe {@code topic}; false when it reports it unknown. */
    private boolean topicExists(String topic) {
        try {
            admin.describeTopics(List.of(topic)).allTopicNames().get(30, java.util.concurrent.TimeUnit.SECONDS);
            return true;
        } catch (ExecutionException e) {
            if (e.getCause() instanceof UnknownTopicOrPartitionException) {
                return false;
            }
            throw new AssertionError("describeTopics(" + topic + ") failed", e);
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            throw new AssertionError("interrupted describing " + topic, e);
        } catch (Exception e) {
            throw new AssertionError("describeTopics(" + topic + ") failed", e);
        }
    }

    /** A plain byte-array producer on the given bootstrap (direct or proxied). Caller closes it. */
    static Producer<byte[], byte[]> newProducer(String bootstrap) {
        Map<String, Object> props = Map.of(
                ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrap,
                ProducerConfig.ACKS_CONFIG, "all",
                ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, ByteArraySerializer.class.getName(),
                ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, ByteArraySerializer.class.getName(),
                // Fail proxied sends fast once the link is cut rather than block the test for minutes.
                ProducerConfig.MAX_BLOCK_MS_CONFIG, "5000",
                ProducerConfig.DELIVERY_TIMEOUT_MS_CONFIG, "5000",
                ProducerConfig.REQUEST_TIMEOUT_MS_CONFIG, "4000");
        return new KafkaProducer<>(props);
    }

    /** A byte-array consumer assigned to all partitions of {@code topic} from the beginning. */
    static Consumer<byte[], byte[]> newAssignedConsumer(String bootstrap, String topic, String isolation) {
        Map<String, Object> props = Map.of(
                ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG,
                bootstrap,
                ConsumerConfig.ISOLATION_LEVEL_CONFIG,
                isolation,
                ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG,
                "false",
                ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG,
                ByteArrayDeserializer.class.getName(),
                ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG,
                ByteArrayDeserializer.class.getName());
        KafkaConsumer<byte[], byte[]> consumer = new KafkaConsumer<>(props);
        List<TopicPartition> assignment = consumer.partitionsFor(topic).stream()
                .map(info -> new TopicPartition(topic, info.partition()))
                .toList();
        consumer.assign(assignment);
        consumer.seekToBeginning(assignment);
        return consumer;
    }

    // ------------------------------------------------------------------ fault helpers (task §1)

    /** Black-holes the link: in-flight and new connections through the proxy fail immediately. */
    static void cut(Proxy proxy) {
        proxy.setConnectionCut(true);
    }

    /** Heals the link and clears any latency/timeout toxics — back to a clean proxied path. */
    static void restore(Proxy proxy) {
        proxy.setConnectionCut(false);
        proxy.removeToxic(LATENCY_TOXIC);
        proxy.removeToxic(TIMEOUT_TOXIC);
    }

    /** Adds {@code ms} of latency in both directions (degrade without a hard partition). */
    static void latency(Proxy proxy, long ms) {
        try {
            proxy.removeToxic(LATENCY_TOXIC);
            proxy.delegate.toxics().latency(LATENCY_TOXIC, ToxicDirection.DOWNSTREAM, ms);
        } catch (IOException e) {
            throw new UncheckedIOException("adding latency toxic failed", e);
        }
    }

    /**
     * Stops all data after {@code ms} of inactivity and holds the connection open (a stall, not a
     * reset) — the shape the I8 barrier-ordering and in-doubt scenarios use to freeze a member.
     */
    static void timeout(Proxy proxy, long ms) {
        try {
            proxy.removeToxic(TIMEOUT_TOXIC);
            proxy.delegate.toxics().timeout(TIMEOUT_TOXIC, ToxicDirection.DOWNSTREAM, ms);
        } catch (IOException e) {
            throw new UncheckedIOException("adding timeout toxic failed", e);
        }
    }

    // ------------------------------------------------------------------ proxy handle

    /**
     * A handle for one Toxiproxy proxy: cut/restore the link and add/remove latency &amp; timeout
     * toxics. Wraps the testcontainers {@link ContainerProxy}; the static {@code cut/restore/...}
     * helpers on {@link ToxiproxySupport} operate on these (the API the task specifies), and the
     * instance methods are the same operations for fluent call sites.
     */
    @SuppressWarnings("deprecation")
    static final class Proxy {
        private final ContainerProxy delegate;

        private Proxy(ContainerProxy delegate) {
            this.delegate = delegate;
        }

        void setConnectionCut(boolean cut) {
            delegate.setConnectionCut(cut);
        }

        void cut() {
            ToxiproxySupport.cut(this);
        }

        void restore() {
            ToxiproxySupport.restore(this);
        }

        void latency(long ms) {
            ToxiproxySupport.latency(this, ms);
        }

        void timeout(long ms) {
            ToxiproxySupport.timeout(this, ms);
        }

        private void removeToxic(String name) {
            try {
                var existing = delegate.toxics().get(name);
                if (existing != null) {
                    existing.remove();
                }
            } catch (IOException e) {
                // A 404 (toxic absent) surfaces as IOException in toxiproxy-java; treat as no-op.
                if (!isNotFound(e)) {
                    throw new UncheckedIOException("removing toxic " + name + " failed", e);
                }
            }
        }

        private static boolean isNotFound(IOException e) {
            String message = e.getMessage();
            return message != null && message.contains("404");
        }
    }

    // ------------------------------------------------------------------ smoke self-check

    /**
     * Round-trips one record through the proxied path: produce&#8594;consume succeed, a {@code cut}
     * makes a produce fail, a {@code restore} makes the next produce&#8594;consume succeed again.
     * Used by the harness smoke test and as a one-call sanity gate for scenario suites.
     */
    void assertProxyRoundTrips() {
        String topic = createTopic("m6-roundtrip", 1);
        byte[] key = "k".getBytes(StandardCharsets.UTF_8);
        try (Producer<byte[], byte[]> producer = newProducer(proxiedBootstrap)) {
            producer.send(new ProducerRecord<>(topic, key, "before".getBytes(StandardCharsets.UTF_8)))
                    .get();
        } catch (Exception e) {
            throw new AssertionError("baseline produce through the proxy failed", e);
        }
        cut(brokerProxy);
        boolean failedWhileCut = false;
        try (Producer<byte[], byte[]> producer = newProducer(proxiedBootstrap)) {
            producer.send(new ProducerRecord<>(topic, key, "during".getBytes(StandardCharsets.UTF_8)))
                    .get();
        } catch (Exception expected) {
            failedWhileCut = true;
        }
        restore(brokerProxy);
        if (!failedWhileCut) {
            throw new AssertionError("produce unexpectedly succeeded while the proxy was cut");
        }
        try (Producer<byte[], byte[]> producer = newProducer(proxiedBootstrap)) {
            producer.send(new ProducerRecord<>(topic, key, "after".getBytes(StandardCharsets.UTF_8)))
                    .get();
        } catch (Exception e) {
            throw new AssertionError("produce after restore failed", e);
        }
        // Exactly the two committed records (before, after) are readable on the direct path.
        int committed = new FencingProbe(directBootstrap).count(topic, "read_committed", Duration.ofSeconds(20));
        assertEquals(2, committed, "expected exactly the before+after records on " + topic);
    }
}
