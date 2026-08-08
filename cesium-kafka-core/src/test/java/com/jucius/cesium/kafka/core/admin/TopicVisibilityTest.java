package com.jucius.cesium.kafka.core.admin;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertSame;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.util.List;
import java.util.Map;
import java.util.Optional;
import org.apache.kafka.common.errors.TopicAuthorizationException;
import org.apache.kafka.common.errors.UnknownTopicOrPartitionException;
import org.junit.jupiter.api.Nested;
import org.junit.jupiter.api.Test;

/** The bounded wait for proven-existing topic metadata (see {@link TopicVisibility}). */
class TopicVisibilityTest {

    private static final String TOPIC = "cesium.app.tracker";

    private final FakeClusterAdmin admin = new FakeClusterAdmin();
    private final VirtualWaits waits = new VirtualWaits();

    private TopicVisibility visibility() {
        return new TopicVisibility(admin, waits.clock(), waits.waiter());
    }

    @Nested
    class AwaitCreated {

        @Test
        void aVisibleTopicCostsOneDescribeAndNoWait() {
            admin.addTopic(TOPIC, 3, Map.of());

            Optional<TopicFacts> facts = visibility().awaitCreated(TOPIC, 3);

            assertTrue(facts.isPresent());
            assertEquals(1, admin.describeCalls.get(TOPIC), "the happy path adds no extra RPC");
            assertEquals(List.of(), waits.waits(), "the happy path never sleeps");
        }

        @Test
        void aLaggingTopicIsRetriedOnACappedExponentialBackoff() {
            admin.addTopic(TOPIC, 3, Map.of());
            admin.invisibleForDescribes(TOPIC, 3);

            TopicVisibility visibility = visibility();
            Optional<TopicFacts> facts = visibility.awaitCreated(TOPIC, 3);

            assertTrue(facts.isPresent(), "the topic became visible within the budget");
            assertEquals(List.of(50L, 100L, 200L), waits.waits());
            assertEquals(350L, visibility.lastWaitedMillis());
        }

        @Test
        void theBackoffIsCappedAtOneSecond() {
            admin.addTopic(TOPIC, 1, Map.of());
            admin.invisibleForDescribes(TOPIC, 8);

            visibility().awaitCreated(TOPIC, 1);

            assertEquals(List.of(50L, 100L, 200L, 400L, 800L, 1_000L, 1_000L, 1_000L), waits.waits());
        }

        @Test
        void aNeverVisibleTopicGivesUpAtTheBudgetWithoutOvershooting() {
            admin.addTopic(TOPIC, 1, Map.of());
            admin.invisibleForDescribes(TOPIC, Integer.MAX_VALUE);

            TopicVisibility visibility = visibility();
            Optional<TopicFacts> facts = visibility.awaitCreated(TOPIC, 1);

            assertTrue(facts.isEmpty());
            assertEquals(TopicVisibility.BUDGET.toMillis(), waits.elapsedMillis(), "waits exactly the budget");
            assertEquals(TopicVisibility.BUDGET.toMillis(), visibility.lastWaitedMillis());
        }

        @Test
        void aPartiallyPropagatedDescriptionIsRetriedRatherThanAccepted() {
            // The topic really has 3 partitions, but the first two describes reassemble only 1 —
            // accepting that would feed the §2.1 parity check a short count.
            admin.addTopic(TOPIC, 3, Map.of());
            admin.shortForDescribes(TOPIC, 2);

            Optional<TopicFacts> facts = visibility().awaitCreated(TOPIC, 3);

            assertTrue(facts.isPresent());
            assertEquals(3, facts.get().partitionCount());
            assertEquals(List.of(50L, 100L), waits.waits());
        }

        @Test
        void aPersistentlyShortDescriptionIsNeverReturnedAsIfComplete() {
            admin.addTopic(TOPIC, 3, Map.of());
            admin.shortForDescribes(TOPIC, Integer.MAX_VALUE);

            assertTrue(visibility().awaitCreated(TOPIC, 3).isEmpty());
        }

        @Test
        void anInterruptRestoresTheFlagAndStopsWaiting() {
            admin.addTopic(TOPIC, 1, Map.of());
            admin.invisibleForDescribes(TOPIC, Integer.MAX_VALUE);
            TopicVisibility visibility = new TopicVisibility(admin, waits.clock(), millis -> {
                throw new InterruptedException("shutdown");
            });

            Optional<TopicFacts> facts = visibility.awaitCreated(TOPIC, 1);

            assertTrue(facts.isEmpty());
            assertTrue(Thread.interrupted(), "the interrupt flag is restored (and cleared here)");
            assertEquals(0L, waits.elapsedMillis(), "a shutdown does not burn the budget");
        }
    }

    @Nested
    class AwaitVisible {

        @Test
        void aTopicIsAwaitedRegardlessOfPartitionCount() {
            admin.addTopic(TOPIC, 3, Map.of());
            admin.invisibleForDescribes(TOPIC, 2);

            assertTrue(visibility().awaitVisible(TOPIC).isPresent());
            assertEquals(List.of(50L, 100L), waits.waits());
        }

        @Test
        void aTopicThatTrulyDoesNotExistReturnsEmptyAfterTheBudget() {
            assertTrue(visibility().awaitVisible("never-created").isEmpty());
            assertEquals(TopicVisibility.BUDGET.toMillis(), waits.elapsedMillis());
        }
    }

    @Nested
    class AwaitTopicConfigs {

        @Test
        void anUnknownTopicErrorIsWaitedOut() {
            admin.addTopic(TOPIC, 1, Map.of("cleanup.policy", "compact"));
            admin.configsUnknownForCalls(TOPIC, 2);

            Map<String, String> configs = visibility().awaitTopicConfigs(TOPIC);

            assertEquals("compact", configs.get("cleanup.policy"));
            assertEquals(List.of(50L, 100L), waits.waits());
        }

        @Test
        void aPersistentUnknownTopicErrorPropagatesAfterTheBudget() {
            admin.addTopic(TOPIC, 1, Map.of());
            admin.configsUnknownForCalls(TOPIC, Integer.MAX_VALUE);

            ClusterAdminException thrown =
                    assertThrows(ClusterAdminException.class, () -> visibility().awaitTopicConfigs(TOPIC));

            assertTrue(thrown.hasCause(UnknownTopicOrPartitionException.class));
            assertEquals(TopicVisibility.BUDGET.toMillis(), waits.elapsedMillis());
        }

        @Test
        void anAuthorizationFailureIsNotRetried() {
            // Waiting out a permanent error would burn the budget AND stack the 30 s per-call
            // timeout underneath it — the operator must hear about this immediately.
            ClusterAdminException denied = new ClusterAdminException(
                    "describeConfigs failed", new TopicAuthorizationException("not authorized"));
            admin.topicConfigsFailure = denied;

            ClusterAdminException thrown =
                    assertThrows(ClusterAdminException.class, () -> visibility().awaitTopicConfigs(TOPIC));

            assertSame(denied, thrown);
            assertEquals(List.of(), waits.waits(), "a permanent failure is never waited on");
        }
    }

    @Test
    void lastWaitedMillisResetsPerCall() {
        admin.addTopic(TOPIC, 1, Map.of());
        admin.invisibleForDescribes(TOPIC, 2);
        TopicVisibility visibility = visibility();

        visibility.awaitCreated(TOPIC, 1);
        assertEquals(150L, visibility.lastWaitedMillis());

        visibility.awaitVisible(TOPIC);
        assertEquals(0L, visibility.lastWaitedMillis(), "the second call saw the topic immediately");
        assertFalse(waits.waits().isEmpty());
    }
}
