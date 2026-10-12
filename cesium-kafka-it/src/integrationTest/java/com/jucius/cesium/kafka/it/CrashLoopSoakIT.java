package com.jucius.cesium.kafka.it;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.junit.jupiter.api.Assertions.fail;

import com.jucius.cesium.kafka.api.headers.CesiumHeaders;
import com.jucius.cesium.kafka.core.kafka.KafkaClientFactory;
import com.jucius.cesium.kafka.core.testing.CrashPoints;
import java.io.IOException;
import java.net.InetAddress;
import java.net.ServerSocket;
import java.nio.file.Files;
import java.nio.file.Path;
import java.time.Duration;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Random;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;
import java.util.concurrent.locks.LockSupport;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Crash-loop soak (design §11.3-12): three engine processes, each a separate JVM running the
 * production app ({@link CrashLoopChild}), take sustained delayed load while the test SIGKILLs them
 * and restarts some with a crash point armed. Afterwards every input must arrive exactly once and
 * never early ({@link DeliveryInvariants}), and the DLQ must stay empty.
 *
 * <p><strong>Kill loop.</strong> Every 5–20 s the test SIGKILLs a live slot ({@code destroyForcibly},
 * exit 137), leaves it down for 0–10 s (a pause longer than the 6 s session timeout moves its
 * partitions), and restarts it under the same instance id. Half the restarts arm one random §3.9
 * point, at which the child halts itself with exit {@value CrashLoopChild#CRASH_EXIT}; the next tick
 * restarts it. The test always waits for a process to exit before reusing its instance id, because
 * two live processes with one id would keep fencing each other. If a child the test did not kill
 * exits any other way, the test fails and names that child's log under
 * {@code build/reports/tests/soak-children/}.
 *
 * <p>Knobs: {@code -Dsoak.crashloop.rate} (records/s, default 200), {@code .minutes} (kill-loop
 * length, default 6), {@code .crashProb} (halt probability per firing of the armed point, default
 * 0.02) and {@code .drainMin} (final drain bound, default 5). The {@code soakPerf} task passes the
 * children's classpath as {@code soak.childClasspath}.
 */
@Tag("soak")
class CrashLoopSoakIT extends KafkaIT {

    private static final Logger log = LoggerFactory.getLogger(CrashLoopSoakIT.class);

    private static final int RATE = Integer.getInteger("soak.crashloop.rate", 200);
    private static final int MINUTES = Integer.getInteger("soak.crashloop.minutes", 6);
    private static final double CRASH_PROB = Double.parseDouble(System.getProperty("soak.crashloop.crashProb", "0.02"));
    private static final int DRAIN_MIN = Integer.getInteger("soak.crashloop.drainMin", 5);
    private static final String CHILD_CLASSPATH = System.getProperty("soak.childClasspath");

    private static final int PARTITIONS = 6;
    private static final int MIN_DELAY_MS = 2_000;
    private static final int MAX_DELAY_MS = 60_000;

    /** What {@link Process#exitValue()} reports for a child ended by SIGKILL (128 + 9). */
    private static final int SIGKILL_EXIT = 137;

    private static final List<String> CRASH_POINTS = List.of(
            CrashPoints.INGEST_BEFORE_BEGIN,
            CrashPoints.INGEST_AFTER_SENDS,
            CrashPoints.INGEST_AFTER_SEND_OFFSETS,
            CrashPoints.INGEST_DURING_COMMIT,
            CrashPoints.DISPATCH_BEFORE_BEGIN,
            CrashPoints.DISPATCH_AFTER_SENDS,
            CrashPoints.DISPATCH_DURING_COMMIT);

    private static final Path LOG_DIR = Path.of("build", "reports", "tests", "soak-children");

    private final Random random = new Random();
    private final List<Slot> slots = List.of(new Slot("s0"), new Slot("s1"), new Slot("s2"));

    /** Ryuk reaps containers, not {@code ProcessBuilder} children, so a dying test JVM kills them. */
    private final Thread reaper = new Thread(this::destroyAll, "crash-loop-reaper");

    private Path config;
    private Load load;
    private int kills;
    private int armedRestarts;
    private int crashHalts;

    @AfterEach
    void tearDown() {
        if (load != null) {
            load.running = false;
        }
        destroyAll();
        try {
            Runtime.getRuntime().removeShutdownHook(reaper);
        } catch (IllegalStateException alreadyShuttingDown) {
            // The hook is running or about to; it does the same work.
        }
    }

    @Test
    void sigkillAndCrashPointLoopKeepsEveryDelayedRecordExactlyOnceAndNeverEarly() throws Exception {
        assertNotNull(CHILD_CLASSPATH, "soak.childClasspath is unset: run through :cesium-kafka-it:soakPerf");
        String source = createTopic("crash-src", PARTITIONS);
        String destination = createTopic("crash-dst", PARTITIONS);
        String dlq = createTopic("crash-dlq", 1);
        String applicationId = unique("crash-app");
        ScenarioSupport.seedIngestOffsets(ADMIN, source, KafkaClientFactory.ingestGroupId(applicationId));
        config = writeConfig(applicationId, source, destination, dlq);

        Runtime.getRuntime().addShutdownHook(reaper);
        long startNanos = System.nanoTime();
        for (Slot slot : slots) {
            start(slot, false);
        }
        load = new Load(source);
        Thread producer = new Thread(load, "crash-loop-producer");
        producer.start();

        long killUntil = System.nanoTime() + TimeUnit.MINUTES.toNanos(MINUTES);
        while (System.nanoTime() < killUntil) {
            Thread.sleep(5_000 + random.nextInt(15_001));
            reap();
            List<Slot> live = slots.stream().filter(s -> s.process.isAlive()).toList();
            if (live.isEmpty()) {
                continue;
            }
            Slot victim = live.get(random.nextInt(live.size()));
            kill(victim);
            Thread.sleep(random.nextInt(10_001));
            start(victim, random.nextBoolean());
        }

        load.running = false;
        producer.join();
        assertNull(load.failure.get(), "a source produce failed");
        // Drain with all three slots up and none armed.
        for (Slot slot : slots) {
            if (slot.armed && slot.process.isAlive()) {
                kill(slot);
                start(slot, false);
            }
        }
        reap();
        int total = load.requestedAt.size();
        assertTrue(total > 0, "the load produced no records; check -Dsoak.crashloop.rate");
        List<ConsumerRecord<byte[], byte[]>> delivered =
                readExactlyCommitted(destination, total, Duration.ofMinutes(DRAIN_MIN), RETRY_GRACE);
        reap(); // no child died during the drain
        long maxLateMs = DeliveryInvariants.check(delivered, total, load.requestedAt::get);
        assertEquals(0, logEndOffsetTotal(dlq), "nothing may reach the DLQ");

        int restarts = slots.stream().mapToInt(s -> s.incarnation - 1).sum();
        log.info(
                "CRASH-LOOP SOAK: {} records at {}/s, {} min of kills: {} SIGKILLs, {} crash-point halts, {} "
                        + "restarts ({} armed); exactly-once and never-early OK; maxLate={}ms; took {}s",
                total,
                RATE,
                MINUTES,
                kills,
                crashHalts,
                restarts,
                armedRestarts,
                maxLateMs,
                TimeUnit.NANOSECONDS.toSeconds(System.nanoTime() - startNanos));
    }

    /**
     * Restarts every child that halted at its armed crash point; fails on any other exit, since the
     * test only kills a child inside {@link #kill}.
     */
    private void reap() throws IOException {
        for (Slot slot : slots) {
            if (!slot.process.isAlive()) {
                int exit = slot.process.exitValue();
                expectExit(slot, exit, exit == CrashLoopChild.CRASH_EXIT && slot.armed);
                start(slot, false);
            }
        }
    }

    /** SIGKILLs the slot's child and waits for it, so its instance id is free to reuse. */
    private void kill(Slot slot) throws InterruptedException {
        slot.process.destroyForcibly();
        int exit = slot.process.waitFor();
        // A child can halt at its armed crash point just before the signal lands.
        expectExit(slot, exit, exit == SIGKILL_EXIT || (exit == CrashLoopChild.CRASH_EXIT && slot.armed));
        if (exit == SIGKILL_EXIT) {
            kills++;
        }
    }

    private void expectExit(Slot slot, int exit, boolean expected) {
        long uptimeMs = TimeUnit.NANOSECONDS.toMillis(System.nanoTime() - slot.startedNanos);
        if (!expected) {
            fail(slot.name() + " exited " + exit + " without being killed (seen " + uptimeMs
                    + " ms after it started); see " + slot.log.toAbsolutePath());
        }
        if (exit == CrashLoopChild.CRASH_EXIT) {
            crashHalts++;
        }
        log.info("{} exited {}, seen {} ms after it started", slot.name(), exit, uptimeMs);
    }

    private void start(Slot slot, boolean armed) throws IOException {
        slot.incarnation++;
        slot.armed = armed;
        slot.log = LOG_DIR.resolve(slot.name() + ".log");
        ProcessBuilder builder = new ProcessBuilder(
                        Path.of(System.getProperty("java.home"), "bin", "java").toString(),
                        "-Xmx384m",
                        "-D" + CrashPoints.ENABLE_PROPERTY + "=true",
                        "-cp",
                        CHILD_CLASSPATH,
                        CrashLoopChild.class.getName(),
                        "--config",
                        config.toString())
                .redirectErrorStream(true)
                .redirectOutput(slot.log.toFile());
        Map<String, String> env = builder.environment();
        // An inherited CESIUM_* variable would override the config (or fail it as an unknown key).
        env.keySet().removeIf(name -> name.startsWith("CESIUM_") || name.startsWith("SOAK_CRASH_"));
        env.put("CESIUM_INSTANCE_ID", slot.id);
        env.put("CESIUM_OBSERVABILITY__PORT", Integer.toString(freePort()));
        String point = "none";
        if (armed) {
            armedRestarts++;
            point = CRASH_POINTS.get(random.nextInt(CRASH_POINTS.size()));
            env.put("SOAK_CRASH_POINT", point);
            env.put("SOAK_CRASH_PROB", Double.toString(CRASH_PROB));
        }
        slot.startedNanos = System.nanoTime();
        slot.process = builder.start();
        log.info("{} started, crash point {}", slot.name(), point);
    }

    private void destroyAll() {
        for (Slot slot : slots) {
            Process process = slot.process;
            if (process != null) {
                process.destroyForcibly();
            }
        }
    }

    /**
     * One YAML for every child; the instance id and observability port come per incarnation from
     * the environment. A 10 s transaction timeout bounds the LSO stall a killed child's open
     * transaction leaves; 6 s is the shared broker's minimum session timeout; the PLAINTEXT broker
     * has no authorizer, so the tracker-ACL check is skipped as in {@link EngineHarness}.
     */
    private static Path writeConfig(String applicationId, String source, String destination, String dlq)
            throws IOException {
        Files.createDirectories(LOG_DIR);
        try (var stale = Files.list(LOG_DIR)) {
            for (Path file : stale.toList()) {
                Files.delete(file);
            }
        }
        Path file = LOG_DIR.resolve("cesium.yaml");
        Files.writeString(
                file,
                """
                application-id: %s
                roles: [ingest, dispatch]
                kafka:
                  group-protocol: classic
                  properties:
                    bootstrap.servers: "%s"
                    session.timeout.ms: "6000"
                    heartbeat.interval.ms: "1500"
                  transactions:
                    timeout: PT10S
                route:
                  source:
                    topic: %s
                  destination:
                    topic: %s
                  tracker:
                    bootstrap: CREATE
                  dlq:
                    topic: %s
                  relay:
                    timestamp: DISPATCH
                observability:
                  bind-address: 127.0.0.1
                startup-checks:
                  tracker-acl: SKIP
                """
                        .formatted(applicationId, bootstrap(), source, destination, dlq));
        return file;
    }

    private static int freePort() throws IOException {
        try (ServerSocket socket = new ServerSocket(0, 0, InetAddress.getLoopbackAddress())) {
            return socket.getLocalPort();
        }
    }

    /** One deployment slot: a stable instance id and its current child process. */
    private static final class Slot {
        final String id;
        volatile Process process;
        int incarnation;
        boolean armed;
        long startedNanos;
        Path log;

        Slot(String id) {
            this.id = id;
        }

        String name() {
            return id + "-" + incarnation;
        }
    }

    /**
     * Writes {@link #RATE} records/s with unique keys, each requesting delivery 2–60 s out, and
     * remembers every requested instant. {@link #requestedAt} is read only after the thread joins.
     */
    private static final class Load implements Runnable {
        private final String source;
        private final Map<String, Long> requestedAt = new HashMap<>();
        private final AtomicReference<Exception> failure = new AtomicReference<>();
        private volatile boolean running = true;

        Load(String source) {
            this.source = source;
        }

        @Override
        public void run() {
            Random random = new Random();
            try (Producer<byte[], byte[]> producer = newProducer()) {
                long tick = System.nanoTime();
                // After n ticks of 100 ms, RATE * n / 10 records are due, so a rate below 10/s or not a
                // multiple of 10 is still honored.
                for (long ticks = 1; running; ticks++) {
                    while (requestedAt.size() < RATE * ticks / 10) {
                        int i = requestedAt.size();
                        String key = "c-" + i;
                        long when = System.currentTimeMillis()
                                + MIN_DELAY_MS
                                + random.nextInt(MAX_DELAY_MS - MIN_DELAY_MS + 1);
                        requestedAt.put(key, when);
                        var unused = producer.send(
                                new ProducerRecord<>(
                                        source,
                                        i % PARTITIONS,
                                        null,
                                        bytes(key),
                                        bytes("v"),
                                        List.of(h(CesiumHeaders.DELIVER_AT, Long.toString(when)))),
                                (metadata, e) -> {
                                    if (e != null) {
                                        failure.compareAndSet(null, e);
                                    }
                                });
                    }
                    tick += TimeUnit.MILLISECONDS.toNanos(100);
                    LockSupport.parkNanos(tick - System.nanoTime());
                }
                producer.flush();
            } catch (RuntimeException e) {
                failure.compareAndSet(null, e);
            }
        }
    }
}
