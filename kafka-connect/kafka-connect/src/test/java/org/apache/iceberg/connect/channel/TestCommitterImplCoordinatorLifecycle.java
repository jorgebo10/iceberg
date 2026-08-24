/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */
package org.apache.iceberg.connect.channel;

import java.lang.reflect.Field;
import java.util.List;
import java.util.Optional;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.stream.Collectors;
import org.apache.iceberg.relocated.com.google.common.collect.ImmutableList;
import org.apache.iceberg.relocated.com.google.common.collect.ImmutableSet;
import org.apache.kafka.clients.admin.ConsumerGroupDescription;
import org.apache.kafka.clients.admin.MemberAssignment;
import org.apache.kafka.clients.admin.MemberDescription;
import org.apache.kafka.clients.consumer.MockConsumer;
import org.apache.kafka.clients.consumer.OffsetResetStrategy;
import org.apache.kafka.clients.producer.MockProducer;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.serialization.ByteArraySerializer;
import org.apache.kafka.common.serialization.StringSerializer;
import org.apache.kafka.connect.sink.SinkTaskContext;
import org.junit.jupiter.api.Test;
import org.mockito.MockedStatic;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.mockStatic;
import static org.mockito.Mockito.when;

/**
 * Demonstrates the coordinator leak of apache/iceberg#16016: after a single rebalance, more than
 * one CoordinatorThread is alive in the same JVM.
 *
 * <p>CommitterImpl.close() decides whether to stop the coordinator by re-deriving leadership from a
 * live Admin describe of the consumer group. But close() runs during the rebalance that is revoking
 * the partitions, so the group is not STABLE and that describe can return members carrying no
 * assignments at all. findFirstTopicPartition() then returns null, containsFirstPartition() logs
 * "found no partitions assigned across all members, cannot determine leader" and returns false,
 * stopCoordinator() is never called — and the next open() elects a fresh coordinator on another
 * task, leaving the old one running.
 *
 * <p>Each surviving coordinator keeps its own commit timer and keeps producing StartCommit, which
 * the workers cannot distinguish from the real one; overlapping commit rounds are what produce
 * duplicate data files (apache/iceberg#17340). In production this shows up as several members in
 * the {@code connect-<connector>-coord} consumer group, all but one owning zero partitions.
 *
 * <p>This is the in-process equivalent of counting "iceberg-coord" threads with jstack.
 */
public class TestCommitterImplCoordinatorLifecycle extends ChannelTestBase {

    // CoordinatorThread.THREAD_NAME, which is private.
    private static final String COORDINATOR_THREAD_NAME = "iceberg-coord";

    private static final TopicPartition TP_0 = new TopicPartition(SRC_TOPIC_NAME, 0);
    private static final TopicPartition TP_1 = new TopicPartition(SRC_TOPIC_NAME, 1);

    private static List<Thread> liveCoordinatorThreads() {
        return Thread.getAllStackTraces().keySet().stream()
                .filter(Thread::isAlive)
                .filter(thread -> COORDINATOR_THREAD_NAME.equals(thread.getName()))
                .collect(Collectors.toList());
    }

    private static MemberDescription member(TopicPartition... partitions) {
        return new MemberDescription(
                null, Optional.empty(), null, null, new MemberAssignment(ImmutableSet.copyOf(partitions)));
    }

    private static void setField(Object target, String name, Object value) throws Exception {
        Field field = CommitterImpl.class.getDeclaredField(name);
        field.setAccessible(true);
        field.set(target, value);
    }

    private static Object getField(Object target, String name) throws Exception {
        Field field = CommitterImpl.class.getDeclaredField(name);
        field.setAccessible(true);
        return field.get(target);
    }

    /**
     * A committer that already believes it is initialized, so initialize() is a no-op and does not
     * replace the mocked KafkaClientFactory with a real one that would need a broker.
     */
    private CommitterImpl newInitializedCommitter(String taskId, SinkTaskContext context)
            throws Exception {
        CommitterImpl committer = new CommitterImpl();
        setField(committer, "catalog", catalog);
        setField(committer, "config", config);
        setField(committer, "context", context);
        setField(committer, "clientFactory", clientFactory);
        setField(committer, "taskId", taskId);
        ((AtomicBoolean) getField(committer, "isInitialized")).set(true);
        return committer;
    }

    @Test
    public void testRebalanceLeavesMoreThanOneCoordinatorThreadAlive() throws Exception {
        // Keep every coordinator idle: poll the control topic, never start a commit.
        when(config.commitIntervalMs()).thenReturn(Integer.MAX_VALUE);
        when(config.commitTimeoutMs()).thenReturn(Integer.MAX_VALUE);

        // Give each Coordinator its own clients, so two live coordinators do not share
        // one MockConsumer/MockProducer.
        when(clientFactory.createConsumer(any()))
                .thenAnswer(invocation -> new MockConsumer<>(OffsetResetStrategy.EARLIEST));
        when(clientFactory.createProducer(any()))
                .thenAnswer(
                        invocation -> {
                            MockProducer<String, byte[]> mockProducer =
                                    new MockProducer<>(false, new StringSerializer(), new ByteArraySerializer());
                            mockProducer.initTransactions();
                            return mockProducer;
                        });

        int before = liveCoordinatorThreads().size();

        SinkTaskContext context = mock(SinkTaskContext.class);
        CommitterImpl taskA = newInitializedCommitter("task-0", context);
        CommitterImpl taskB = newInitializedCommitter("task-1", context);

        try (MockedStatic<KafkaUtils> mockKafkaUtils = mockStatic(KafkaUtils.class)) {
            ConsumerGroupDescription groupDesc = mock(ConsumerGroupDescription.class);
            mockKafkaUtils
                    .when(() -> KafkaUtils.consumerGroupDescription(any(), any()))
                    .thenReturn(groupDesc);

            // 1. STABLE group. Task A holds the lowest partition, so A is the leader
            //    and starts the one coordinator that should exist.
            when(groupDesc.members()).thenReturn(ImmutableList.of(member(TP_0), member(TP_1)));
            taskA.open(catalog, config, context, ImmutableList.of(TP_0));
            assertThat(liveCoordinatorThreads()).hasSize(before + 1);

            taskB.open(catalog, config, context, ImmutableList.of(TP_1));
            assertThat(liveCoordinatorThreads()).hasSize(before + 1);


            // 2. A rebalance starts. This is the crux: while partitions are being
            //    revoked the group is not STABLE, and the Admin describe reports
            //    members with no assignments at all.
            when(groupDesc.members())
                    .thenReturn(ImmutableList.of(member(/* no assignment */), member()));

            // 3. Task A is closed as part of that rebalance and loses TP_0.
            taskA.close(ImmutableList.of(TP_0));
            taskB.close(ImmutableList.of(TP_1));


            // 4. The rebalance settles and task B now holds the lowest partition, so B
            //    elects itself and starts a coordinator of its own.
            when(groupDesc.members()).thenReturn(ImmutableList.of(member(TP_0), member(TP_1)));
            taskB.open(catalog, config, context, ImmutableList.of(TP_0));
            //Task A lost its coordinator, but it was never stopped because the
            // Admin describe returned no assignments during the rebalance.
            // So now there are two coordinators alive.
            taskA.open(catalog, config, context, ImmutableList.of(TP_1));


            // Exactly one coordinator must be alive for the connector. On 1.11.0 two
            // are: task A's was never stopped in step 3.
            assertThat(liveCoordinatorThreads())
                    .as(
                            "after one rebalance there must still be exactly one live %s thread; "
                                    + "more than one means close() orphaned task A's coordinator",
                            COORDINATOR_THREAD_NAME)
                    .hasSize(before + 1);
        } finally {
            stopQuietly(taskA);
            stopQuietly(taskB);
        }
    }

    /** Terminates whatever coordinator a committer still owns, so threads do not leak. */
    private void stopQuietly(CommitterImpl committer) throws Exception {
        CoordinatorThread thread = (CoordinatorThread) getField(committer, "coordinatorThread");
        if (thread != null) {
            thread.terminate();
        }
    }
}
