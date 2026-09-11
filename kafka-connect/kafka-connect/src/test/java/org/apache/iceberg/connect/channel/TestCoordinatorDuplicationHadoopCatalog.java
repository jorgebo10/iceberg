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

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.time.OffsetDateTime;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.stream.Collectors;
import java.util.stream.Stream;
import org.apache.hadoop.conf.Configuration;
import org.apache.iceberg.DataFile;
import org.apache.iceberg.DataFiles;
import org.apache.iceberg.FileFormat;
import org.apache.iceberg.FileScanTask;
import org.apache.iceberg.Schema;
import org.apache.iceberg.Table;
import org.apache.iceberg.catalog.Namespace;
import org.apache.iceberg.catalog.TableIdentifier;
import org.apache.iceberg.connect.IcebergSinkConfig;
import org.apache.iceberg.connect.TableSinkConfig;
import org.apache.iceberg.connect.events.AvroUtil;
import org.apache.iceberg.connect.events.DataComplete;
import org.apache.iceberg.connect.events.DataWritten;
import org.apache.iceberg.connect.events.Event;
import org.apache.iceberg.connect.events.PayloadType;
import org.apache.iceberg.connect.events.StartCommit;
import org.apache.iceberg.connect.events.TableReference;
import org.apache.iceberg.connect.events.TopicPartitionOffset;
import org.apache.iceberg.exceptions.CommitFailedException;
import org.apache.iceberg.hadoop.HadoopCatalog;
import org.apache.iceberg.relocated.com.google.common.collect.ImmutableList;
import org.apache.iceberg.relocated.com.google.common.collect.ImmutableMap;
import org.apache.iceberg.relocated.com.google.common.collect.ImmutableSet;
import org.apache.iceberg.relocated.com.google.common.collect.Lists;
import org.apache.iceberg.types.Types;
import org.apache.iceberg.types.Types.StructType;
import org.apache.kafka.clients.admin.Admin;
import org.apache.kafka.clients.admin.DescribeTopicsResult;
import org.apache.kafka.clients.admin.MemberAssignment;
import org.apache.kafka.clients.admin.MemberDescription;
import org.apache.kafka.clients.admin.TopicDescription;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.MockConsumer;
import org.apache.kafka.clients.consumer.OffsetAndMetadata;
import org.apache.kafka.clients.consumer.OffsetResetStrategy;
import org.apache.kafka.clients.producer.MockProducer;
import org.apache.kafka.common.KafkaException;
import org.apache.kafka.common.KafkaFuture;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.TopicPartitionInfo;
import org.apache.kafka.common.serialization.ByteArraySerializer;
import org.apache.kafka.common.serialization.StringSerializer;
import org.apache.kafka.connect.sink.SinkTaskContext;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.mockito.ArgumentMatchers;

import static org.apache.iceberg.types.Types.NestedField.optional;
import static org.apache.iceberg.types.Types.NestedField.required;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.assertj.core.api.Assertions.entry;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyCollection;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.spy;
import static org.mockito.Mockito.when;

/**
 * Same duplication scenarios as {@link TestCoordinatorDuplication}, but against a real {@link
 * HadoopCatalog} on a temp warehouse instead of the in-memory catalog, so the file-system commit
 * protocol runs for real: each commit writes {@code v<N+1>.metadata.json} and swaps {@code
 * version-hint.text} (spec § File System Tables).
 */
public class TestCoordinatorDuplicationHadoopCatalog {

    private static final String CTL_TOPIC_NAME = "ctl-topic";
    private static final String SRC_TOPIC_NAME = "src-topic";
    private static final String CONNECT_CONSUMER_GROUP_ID = "cg-connect";
    private static final Namespace NAMESPACE = Namespace.of("db");
    private static final TableIdentifier TABLE_IDENTIFIER = TableIdentifier.of(NAMESPACE, "tbl");
    private static final String OFFSETS_SNAPSHOT_PROP = String.format("kafka.connect.offsets.%s.%s", CTL_TOPIC_NAME, CONNECT_CONSUMER_GROUP_ID);
    private static final String COMMIT_ID_SNAPSHOT_PROP = "kafka.connect.commit-id";
    private static final String DUPLICATED_PATH = "path/to/file.parquet";
    private static final Schema SCHEMA = new Schema(required(1, "id", Types.LongType.get()), optional(2, "data", Types.StringType.get()), required(3, "date", Types.StringType.get()));

    private Path warehouse;
    private HadoopCatalog catalog;
    private Table table;
    private IcebergSinkConfig config;
    private KafkaClientFactory clientFactory;
    private MockProducer<String, byte[]> producer;
    private MockConsumer<String, byte[]> consumer;

    @BeforeEach
    @SuppressWarnings("deprecation")
    void before() throws IOException {
        warehouse = Files.createTempDirectory("iceberg-connect-test");
        catalog = spy(new HadoopCatalog(new Configuration(), warehouse.toString()));
        catalog.createNamespace(NAMESPACE);
        table = catalog.createTable(TABLE_IDENTIFIER, SCHEMA);

        config = mock(IcebergSinkConfig.class);
        when(config.controlTopic()).thenReturn(CTL_TOPIC_NAME);
        when(config.commitThreads()).thenReturn(1);
        when(config.connectGroupId()).thenReturn(CONNECT_CONSUMER_GROUP_ID);
        when(config.tableConfig(any())).thenReturn(mock(TableSinkConfig.class));
        when(config.commitTimeoutMs()).thenReturn(Integer.MAX_VALUE);
        when(config.commitMaxConsecutiveFailures()).thenReturn(5);

        TopicPartitionInfo partitionInfo = mock(TopicPartitionInfo.class);
        when(partitionInfo.partition()).thenReturn(0);
        DescribeTopicsResult describeResult = mock(DescribeTopicsResult.class);
        when(describeResult.values()).thenReturn(ImmutableMap.of(SRC_TOPIC_NAME, KafkaFuture.completedFuture(new TopicDescription(SRC_TOPIC_NAME, false, ImmutableList.of(partitionInfo)))));
        Admin admin = mock(Admin.class);
        when(admin.describeTopics(anyCollection())).thenReturn(describeResult);

        producer = new MockProducer<>(false, new StringSerializer(), new ByteArraySerializer());
        producer.initTransactions();
        consumer = new MockConsumer<>(OffsetResetStrategy.EARLIEST);

        clientFactory = mock(KafkaClientFactory.class);
        when(clientFactory.createProducer(any())).thenReturn(producer);
        when(clientFactory.createConsumer(any())).thenReturn(consumer);
        when(clientFactory.createAdmin()).thenReturn(admin);
    }

    @AfterEach
    void after() throws IOException {
        catalog.close();
    }

    /** Replay at a NEW control-topic offset: committed again, and the catalog version advances. */
    @Test
    public void duplicateAtNewOffsetsIsCommittedTwice() throws IOException {
        Coordinator coordinator = newCoordinator();
        DataFile dataFile = EventTestUtil.createDataFile();

        // v1.metadata.json is the createTable above
        assertThat(versionHint()).isEqualTo(1);

        commitCycle(coordinator, dataFile, 1L, 2L);
        table.refresh();
        assertThat(versionHint()).isEqualTo(2);
        assertThat(table.currentSnapshot().summary()).containsEntry(OFFSETS_SNAPSHOT_PROP, "{\"0\":3}").containsEntry("total-data-files", "1");

        commitCycle(coordinator, dataFile, 3L, 4L);
        table.refresh();

        // one metadata version per commit, and the catalog pointer moved with it
        assertThat(versionHint()).isEqualTo(3);
        assertThat(metadataFiles()).contains("v1.metadata.json", "v2.metadata.json", "v3.metadata.json");

        assertThat(table.snapshots()).hasSize(2);
        assertThat(table.currentSnapshot().summary()).containsEntry(OFFSETS_SNAPSHOT_PROP, "{\"0\":5}").containsEntry("total-data-files", "2");

        // the duplication, on a real catalog
        assertThat(scannedLocations()).containsExactly(entry(DUPLICATED_PATH, 2L));
        assertThat(eventTypes()).filteredOn(PayloadType.COMMIT_TO_TABLE::equals).hasSize(2);

        // two distinct commit ids, one per cycle: the signature of a re-announced DataWritten
        assertThat(commitIds()).hasSize(2).doesNotHaveDuplicates();
    }

    /**
     * Replay of already-committed control records: filtered, and no new metadata version is written
     * at all.
     *
     * <p>Getting such a replay takes more than a rebalance. The first cycle succeeded, so {@code
     * commitConsumerOffsets()} ran and the {@code -coord} group sits at 3 — a rejoining consumer
     * would resume there and replay nothing. This is the case where the **group committed offsets are
     * gone**: expired via {@code offsets.retention.minutes} after a long stop, reset by hand, or the
     * group recreated. Then {@code auto.offset.reset} rewinds, which MockConsumer does not emulate on
     * rebalance, so it is expressed here with seekToBeginning.
     */
    @Test
    public void replayAfterLostGroupOffsetsIsFiltered() throws IOException {
        Coordinator coordinator = newCoordinator();
        DataFile dataFile = EventTestUtil.createDataFile();

        commitCycle(coordinator, dataFile, 1L, 2L);
        table.refresh();
        assertThat(versionHint()).isEqualTo(2);

        consumer.seekToBeginning(ImmutableList.of(new TopicPartition(CTL_TOPIC_NAME, 0)));
        commitCycle(coordinator, dataFile, 1L, 2L);
        table.refresh();

        // no append, so no new metadata version at all
        assertThat(versionHint()).isEqualTo(2);
        assertThat(metadataFiles()).doesNotContain("v3.metadata.json");
        assertThat(table.snapshots()).hasSize(1);
        assertThat(scannedLocations()).containsExactly(entry(DUPLICATED_PATH, 1L));

        // the replay was consumed: second cycle ran, committed nothing to the table
        assertThat(eventTypes()).containsExactly(PayloadType.START_COMMIT, PayloadType.COMMIT_TO_TABLE, PayloadType.COMMIT_COMPLETE, PayloadType.START_COMMIT, PayloadType.COMMIT_COMPLETE);
    }

    /**
     * The sink's own default is {@code auto.offset.reset=latest} (`KafkaClientFactory#createConsumer`
     * sets it with putIfAbsent), and `ChannelTestBase` hides that by building its mock with EARLIEST.
     * With the default, a coordinator that starts with no committed offsets for its `-coord` group
     * jumps to the END of the control topic and never sees announcements already sitting there: the
     * files are never committed, while the workers' data-topic offsets were already committed
     * transactionally alongside those DataWritten events.
     *
     * <p>That is data loss, the mirror image of the duplication cases. It is why
     * `iceberg.kafka.auto.offset.reset: earliest` matters in the deployed config.
     */
    @Test
    public void defaultLatestResetSkipsAnnouncementsAlreadyInTheControlTopic() throws IOException {
        // rebuild the consumer with the sink's real default
        consumer = new MockConsumer<>(OffsetResetStrategy.LATEST);
        when(clientFactory.createConsumer(any())).thenReturn(consumer);

        SinkTaskContext context = mock(SinkTaskContext.class);
        Coordinator coordinator = new Coordinator(catalog, config, ImmutableList.of(), clientFactory, context);
        coordinator.start();

        TopicPartition tp = new TopicPartition(CTL_TOPIC_NAME, 0);
        consumer.rebalance(ImmutableList.of(tp));
        consumer.updateEndOffsets(ImmutableMap.of(tp, 3L));

        // a worker announced these before this coordinator joined, and nobody committed the -coord
        // group
        UUID orphanCommit = UUID.fromString("00000000-0000-0000-0000-0000000000ff");
        addControlRecord(1L, new Event(config.connectGroupId(), new DataWritten(StructType.of(), orphanCommit, TableReference.of("catalog", TABLE_IDENTIFIER, null), ImmutableList.of(EventTestUtil.createDataFile()), ImmutableList.of())));
        addControlRecord(2L, new Event(config.connectGroupId(), new DataComplete(orphanCommit, ImmutableList.of(new TopicPartitionOffset("topic", 1, 1L, EventTestUtil.now())))));

        coordinator.process();
        coordinator.process();
        table.refresh();

        // the consumer jumped straight to the end, so those records were never delivered
        assertThat(consumer.position(tp)).isEqualTo(3L);
        assertThat(table.snapshots()).isEmpty();
        assertThat(versionHint()).isEqualTo(1);
        assertThat(eventTypes()).containsOnly(PayloadType.START_COMMIT);
    }

    /**
     * The table commit lands but committing the {@code -coord} consumer offsets fails right after
     * (`Channel#commitConsumerOffsets`), injected with a Mockito spy over the MockConsumer — no test
     * seam in production code.
     *
     * <p>Consequences: the snapshot exists, {@code CommitComplete} is never sent, the group committed
     * offsets stay behind and the envelopes stay buffered. The replay that follows is still filtered,
     * because the watermark went into the table in the same commit that wrote the data.
     */
    @Test
    public void consumerOffsetCommitFailsAfterTableCommit() throws IOException {
        consumer = spy(new MockConsumer<>(OffsetResetStrategy.EARLIEST));
        when(clientFactory.createConsumer(any())).thenReturn(consumer);
        doThrow(new KafkaException("offset commit failed")).doCallRealMethod().when(consumer).commitSync(ArgumentMatchers.<Map<TopicPartition, OffsetAndMetadata>>any());

        Coordinator coordinator = newCoordinator();
        DataFile dataFile = EventTestUtil.createDataFile();
        TopicPartition tp = new TopicPartition(CTL_TOPIC_NAME, 0);

        // Inline the first cycle: table commit succeeds, but commitSync throws KafkaException.
        // KafkaException is not a CommitFailedException, so commit() re-throws it immediately.
        // endCurrentCommit() still runs in finally, so commitBuffer is NOT cleared (the DataWritten
        // envelope stays buffered for the next cycle) but the snapshot IS written to the table.
        coordinator.process();
        UUID commit1 = lastStartCommitId();
        sendDataWritten(commit1, dataFile, 1L);
        sendDataComplete(commit1, 2L);
        assertThatThrownBy(() -> coordinator.process()).isInstanceOf(KafkaException.class).hasMessage("offset commit failed");

        table.refresh();

        // the table committed: snapshot and new metadata version are there
        assertThat(versionHint()).isEqualTo(2);
        assertThat(table.snapshots()).hasSize(1);
        assertThat(table.currentSnapshot().summary()).containsEntry(OFFSETS_SNAPSHOT_PROP, "{\"0\":3}");
        // but the cycle never finished: no CommitComplete, group commit offsets not advanced
        assertThat(eventTypes()).doesNotContain(PayloadType.COMMIT_COMPLETE);
        // committed offsets are empty because the commitSync threw an exception
        assertThat(consumer.committed(ImmutableSet.of(tp))).isEmpty();

        // a rebalance follows. There is no committed offset for the -coord group, so the rejoining
        // consumer falls back to auto.offset.reset — EARLIEST here, i.e. offset 0, not the offset the
        // failed commit would have written. No manual seek needed: the reset strategy does it.
        consumer.seekToBeginning(ImmutableList.of(tp));
        commitCycle(coordinator, dataFile, 1L, 2L);
        table.refresh();

        // the replay was really consumed: the second cycle ran a commit and completed it, this time
        // committing nothing to the table
        assertThat(eventTypes()).filteredOn(PayloadType.COMMIT_COMPLETE::equals).hasSize(1);
        assertThat(eventTypes()).filteredOn(PayloadType.COMMIT_TO_TABLE::equals).hasSize(1);

        assertThat(versionHint()).isEqualTo(2);
        assertThat(scannedLocations()).containsExactly(entry(DUPLICATED_PATH, 1L));
    }

    /**
     * CASE 2 — the dedup property key changes, so the filter finds no offsets at all and keeps
     * everything. The key is {@code kafka.connect.offsets.<controlTopic>.<connectGroupId>}, built in
     * the Coordinator constructor, so renaming the control topic — or recreating the connector under
     * another group id — silently disables deduplication for files already in the table.
     */
    @Test
    public void renamedControlTopicLosesTheDedupKeyAndDuplicates() throws IOException {
        DataFile dataFile = EventTestUtil.createDataFile();

        commitCycle(newCoordinator(), dataFile, 1L, 2L);
        table.refresh();
        assertThat(versionHint()).isEqualTo(2);
        assertThat(table.currentSnapshot().summary()).containsEntry(OFFSETS_SNAPSHOT_PROP, "{\"0\":3}");

        // the control topic is renamed; a fresh coordinator looks for a property key the table has
        // never seen, so lastCommittedOffsets() returns an empty map and nothing is filtered
        String renamedTopic = CTL_TOPIC_NAME + "-v2";
        when(config.controlTopic()).thenReturn(renamedTopic);
        Coordinator coordinator = newCoordinatorOn(renamedTopic);

        // the very same DataWritten, replayed at the very same offsets it had before
        commitCycleOn(renamedTopic, coordinator, dataFile, 1L, 2L);
        table.refresh();

        assertThat(versionHint()).isEqualTo(3);
        assertThat(table.snapshots()).hasSize(2);
        assertThat(scannedLocations()).containsExactly(entry(DUPLICATED_PATH, 2L));
        assertThat(table.currentSnapshot().summary()).containsEntry(String.format("kafka.connect.offsets.%s.%s", renamedTopic, CONNECT_CONSUMER_GROUP_ID), "{\"0\":3}");
    }

    /**
     * CASE 3 — the recorded offsets regress below what the table already contains, e.g. an
     * out-of-band commit from a repair job or from a writer with a stale view. The filter then lets
     * already-committed envelopes through, and {@code offsetValidator} cannot tell: the expected
     * offsets it CASes on are the regressed ones.
     */
    @Test
    public void regressedOffsetsLetAlreadyCommittedFilesThroughAgain() throws IOException {
        DataFile dataFile = EventTestUtil.createDataFile();

        Coordinator coordinator = newCoordinator();
        commitCycle(coordinator, dataFile, 1L, 2L);
        table.refresh();
        assertThat(versionHint()).isEqualTo(2);
        assertThat(table.currentSnapshot().summary()).containsEntry(OFFSETS_SNAPSHOT_PROP, "{\"0\":3}");

        // out-of-band commit that puts the recorded offsets back before the envelope
        table.newAppend().appendFile(dataFile("path/to/out-of-band.parquet")).set(OFFSETS_SNAPSHOT_PROP, "{\"0\":1}").commit();
        table.refresh();
        assertThat(versionHint()).isEqualTo(3);

        // replay of the original DataWritten, at its original offsets
        consumer.seekToBeginning(ImmutableList.of(new TopicPartition(CTL_TOPIC_NAME, 0)));
        commitCycle(coordinator, dataFile, 1L, 2L);
        table.refresh();

        assertThat(versionHint()).isEqualTo(4);
        assertThat(scannedLocations()).containsEntry(DUPLICATED_PATH, 2L);
    }

    /**
     * A commit to the table fails and nothing else happens: no restart, no rebalance. The consumer
     * keeps using its in-memory position — the exception rewinds neither the position nor the
     * committed offset — and the envelope of the failed commit survives in {@code commitBuffer}, so
     * it is committed in the next cycle together with whatever arrived meanwhile. No data loss, and
     * the only trace is that the snapshot's {@code kafka.connect.commit-id} belongs to the later
     * cycle while the snapshot also carries the earlier cycle's file.
     */
    @Test
    public void failedCommitKeepsConsumingFromPositionAndRetriesNextCycle() throws IOException {
        Coordinator coordinator = newCoordinator();
        DataFile fileF = EventTestUtil.createDataFile();
        DataFile fileG = dataFile("path/to/g.parquet");
        TopicPartition tp = new TopicPartition(CTL_TOPIC_NAME, 0);

        // cycle 1: a retryable CommitFailedException from loadTable fires before the table is touched.
        // CommitFailedException is the one exception commit() swallows and retries (the coordinator
        // stays alive and keeps consuming from its current position).
        doThrow(new CommitFailedException("catalog unavailable")).doCallRealMethod().when(catalog).loadTable(TABLE_IDENTIFIER);

        coordinator.process();
        UUID commit1 = lastStartCommitId();
        sendDataWritten(commit1, fileF, 1L);
        sendDataComplete(commit1, 2L);
        coordinator.process();

        table.refresh();
        assertThat(table.snapshots()).isEmpty();
        assertThat(versionHint()).isEqualTo(1);
        assertThat(eventTypes()).doesNotContain(PayloadType.COMMIT_TO_TABLE);
        // the in-memory position moved past both records and is NOT rewound by the failure
        assertThat(consumer.position(tp)).isEqualTo(3L);
        // and nothing was committed for the -coord group: commitConsumerOffsets() never ran
        assertThat(consumer.committed(ImmutableSet.of(tp))).isEmpty();

        // cycle 2: consumption continues from the position, so the new records land at 3 and 4
        coordinator.process();
        UUID commit2 = lastStartCommitId();
        sendDataWritten(commit2, fileG, 3L);
        sendDataComplete(commit2, 4L);
        coordinator.process();

        table.refresh();
        assertThat(versionHint()).isEqualTo(2);
        assertThat(table.snapshots()).hasSize(1);
        // the retained envelope means F is committed here too, together with G
        assertThat(scannedLocations()).containsOnlyKeys(DUPLICATED_PATH, "path/to/g.parquet");
        assertThat(table.currentSnapshot().summary()).containsEntry(OFFSETS_SNAPSHOT_PROP, "{\"0\":5}").containsEntry(COMMIT_ID_SNAPSHOT_PROP, commit2.toString());
        assertThat(commit1).isNotEqualTo(commit2);
    }

    private void sendDataWritten(UUID commitId, DataFile dataFile, long offset) {
        addControlRecord(offset, new Event(config.connectGroupId(), new DataWritten(StructType.of(), commitId, TableReference.of("catalog", TABLE_IDENTIFIER, null), ImmutableList.of(dataFile), ImmutableList.of())));
    }

    private void sendDataComplete(UUID commitId, long offset) {
        sendDataComplete(commitId, 1, offset);
    }

    private void sendDataComplete(UUID commitId, int srcPartition, long offset) {
        addControlRecord(offset, new Event(config.connectGroupId(), new DataComplete(commitId, ImmutableList.of(new TopicPartitionOffset("topic", srcPartition, 1L, EventTestUtil.now())))));
    }

    /**
     * A faithful group rebalance on a live consumer: the partition is revoked and reassigned, and the
     * rejoining consumer resumes from the **committed** offset of its group. MockConsumer does not
     * emulate that, so the resume point is read from {@code committed()} and applied with a seek.
     *
     * <p>Because the previous cycle succeeded, {@code commitConsumerOffsets()} had advanced the group
     * to 3 — so the records at 1 and 2 are never served again and there is nothing to filter. This is
     * the control for "an ordinary rebalance does not replay, hence cannot duplicate".
     */
    @Test
    public void rebalanceResumesAtCommittedOffsetSoNothingIsReplayed() throws IOException {
        Coordinator coordinator = newCoordinator();
        DataFile dataFile = EventTestUtil.createDataFile();
        TopicPartition tp = new TopicPartition(CTL_TOPIC_NAME, 0);

        commitCycle(coordinator, dataFile, 1L, 2L);
        table.refresh();
        assertThat(versionHint()).isEqualTo(2);

        // the successful cycle did advance the group: that is the resume point of a rebalance
        assertThat(consumer.committed(ImmutableSet.of(tp))).containsEntry(tp, new OffsetAndMetadata(3L));

        // revoke + reassign, then resume where the group says, exactly like a rejoining consumer
        consumer.rebalance(ImmutableList.of(tp));
        consumer.seek(tp, consumer.committed(ImmutableSet.of(tp)).get(tp).offset());

        // the same records are served again by the broker...
        commitCycle(coordinator, dataFile, 1L, 2L);
        table.refresh();

        // ...but they sit below the resume point, so they are never even delivered: the position never
        // moved back, no second COMMIT_TO_TABLE, no new metadata version, no duplication
        assertThat(consumer.position(tp)).isEqualTo(3L);
        assertThat(eventTypes()).filteredOn(PayloadType.COMMIT_TO_TABLE::equals).hasSize(1);
        assertThat(versionHint()).isEqualTo(2);
        assertThat(table.snapshots()).hasSize(1);
        assertThat(scannedLocations()).containsExactly(entry(DUPLICATED_PATH, 1L));
    }

    /**
     * The in-memory control-topic watermark is updated with a plain {@code put}
     * (`Channel#consumeAvailable`), never with a max, so consuming replayed records LOWERS it. The
     * table is shielded by the {@code Long::max} merge in `Coordinator#commitToTable`, but {@code
     * commitConsumerOffsets()} commits the in-memory map as is — so the durable group checkpoint
     * moves BACKWARDS. No duplication follows here, only future replays.
     */
    @Test
    public void replayLowersTheInMemoryWatermarkAndRegressesTheGroupCheckpoint() throws IOException {
        Coordinator coordinator = newCoordinator();
        DataFile fileA = EventTestUtil.createDataFile();
        DataFile fileB = dataFile("path/to/b.parquet");
        TopicPartition tp = new TopicPartition(CTL_TOPIC_NAME, 0);

        coordinator.process();
        UUID commit1 = lastStartCommitId();
        sendDataWritten(commit1, fileA, 1L);
        sendDataComplete(commit1, 2L);
        coordinator.process();

        coordinator.process();
        UUID commit2 = lastStartCommitId();
        sendDataWritten(commit2, fileB, 3L);
        sendDataComplete(commit2, 4L);
        coordinator.process();
        table.refresh();

        // steady state: table watermark and group checkpoint both at 5
        assertThat(table.currentSnapshot().summary()).containsEntry(OFFSETS_SNAPSHOT_PROP, "{\"0\":5}");
        assertThat(consumer.committed(ImmutableSet.of(tp))).containsEntry(tp, new OffsetAndMetadata(5L));

        // now the group offsets are lost and consumption rewinds to 0: the broker serves the WHOLE
        // control topic again — records 1, 2, 3 and 4 — carrying their ORIGINAL commit ids
        consumer.seekToBeginning(ImmutableList.of(tp));
        sendDataWritten(commit1, fileA, 1L);
        sendDataComplete(commit1, 2L);
        sendDataWritten(commit2, fileB, 3L);
        sendDataComplete(commit2, 4L);

        coordinator.process(); // starts commit3 and drains all four replayed records
        UUID commit3 = lastStartCommitId();
        sendDataComplete(commit3, 5L);
        coordinator.process();
        table.refresh();

        // the replayed files were filtered, so the table did not change at all
        assertThat(versionHint()).isEqualTo(3);
        assertThat(scannedLocations()).containsOnlyKeys(DUPLICATED_PATH, "path/to/b.parquet");
        // and its watermark held, thanks to Long::max in the merge
        assertThat(table.currentSnapshot().summary()).containsEntry(OFFSETS_SNAPSHOT_PROP, "{\"0\":5}");

        // and the group checkpoint moved forward, 5 → 6: the full replay left the in-memory map at its
        // maximum before any commit fired
        assertThat(consumer.committed(ImmutableSet.of(tp))).containsEntry(tp, new OffsetAndMetadata(6L));
    }

    /**
     * A rebalance mid-commit rewinds the control-topic consumer to the {@code -coord} group's
     * committed offset (absent here, so back to 0). The eager protocol discards the in-flight
     * position even when the same partition comes straight back.
     *
     * <p>Fix 1 ({@code CommitState}): readiness is tracked with a {@code Set} of distinct source
     * partitions, so replaying {@code DataComplete(p0)} does not increment the count a second time
     * — no premature commit.
     *
     * <p>Fix 2 ({@code Channel}): {@code controlTopicOffsets} uses {@code merge(max)} instead of
     * {@code put}, so consuming replayed records never lowers the in-memory watermark — the
     * snapshot records the correct high-water mark and the dedup filter stays accurate.
     */
    @Test
    public void rebalanceMidCommitReplaysDataCompleteAndDuplicates() throws IOException {
        // Two source partitions: the coordinator waits for both to report before committing.
        MemberDescription member = new MemberDescription("member-0", "client-0", "host-0",
                new MemberAssignment(ImmutableSet.of(
                        new TopicPartition(SRC_TOPIC_NAME, 0),
                        new TopicPartition(SRC_TOPIC_NAME, 1))));
        Coordinator coordinator = new Coordinator(catalog, config, ImmutableList.of(member), clientFactory, mock(SinkTaskContext.class));
        coordinator.start();

        TopicPartition tp = new TopicPartition(CTL_TOPIC_NAME, 0);
        consumer.rebalance(ImmutableList.of(tp));
        consumer.updateBeginningOffsets(ImmutableMap.of(tp, 0L));

        DataFile fileX = EventTestUtil.createDataFile();
        DataFile fileY = dataFile("path/to/y.parquet");

        coordinator.process();
        UUID commit1 = lastStartCommitId();

        // Worker 0's Kafka transaction: DataWritten + DataComplete in one atomic write.
        sendDataWritten(commit1, fileX, 1L);
        sendDataComplete(commit1, 0, 2L);  // source partition 0 reports — receivedPartitions: {p0}

        // Worker 1's Kafka transaction: also a single atomic write (DataWritten at offset 3,
        // DataComplete at offset 4). The Kafka consumer's poll fetch returned only offset 3
        // before the rebalance fired; offset 4 was committed to the topic but not yet polled.
        sendDataWritten(commit1, fileY, 3L);

        coordinator.process();  // offsets 1–3; receivedPartitions={p0} (size 1 < 2) → no commit
                                 // controlTopicOffsets = {0:4}

        table.refresh();
        assertThat(table.snapshots()).isEmpty();
        assertThat(consumer.committed(ImmutableSet.of(tp))).isEmpty();

        // Eager rebalance: coordinator seeks to committed offset (absent → 0).
        // The next poll fetch returns only offsets 1–2 (fetch boundary before 3).
        consumer.seekToBeginning(ImmutableList.of(tp));
        sendDataWritten(commit1, fileX, 1L);   // replay
        sendDataComplete(commit1, 0, 2L);      // replay of p0 DataComplete

        coordinator.process();  // offsets 1–2 (replay)
                                 // Fix 1: p0 already in receivedPartitions → size stays 1 < 2 → no premature commit
                                 // Fix 2: merge(max) keeps controlTopicOffsets at {0:4}, not regressed to {0:3}
        table.refresh();

        assertThat(eventTypes()).doesNotContain(PayloadType.COMMIT_TO_TABLE);
        assertThat(table.snapshots()).isEmpty();  // Fix 1: no premature commit

        // The rest of worker 1's transaction now arrives (DataComplete at offset 4).
        sendDataComplete(commit1, 1, 4L);

        coordinator.process();  // p1 added → receivedPartitions={p0,p1} (size 2 = 2) → commit fires
        table.refresh();

        assertThat(table.snapshots()).hasSize(1);
        // Fix 2: watermark not regressed — stored at {0:5} (max of all consumed offsets + 1)
        assertThat(table.currentSnapshot().summary()).containsEntry(OFFSETS_SNAPSHOT_PROP, "{\"0\":5}");
        // fileY committed exactly once — no duplication
        assertThat(scannedLocations()).containsEntry("path/to/y.parquet", 1L);
    }

    /**
     * Control test for {@link #rebalanceMidCommitReplaysDataCompleteAndDuplicates}: when the
     * rebalance causes the leader partition to move, the coordinator is torn down and a new one
     * starts fresh. All in-memory state ({@code commitBuffer}, {@code receivedPartitionCount}) is
     * cleared, so the replayed {@code DataComplete} records are not double-counted and no premature
     * commit fires. fileY is committed exactly once — no duplication.
     */
    @Test
    public void coordinatorRestartOnLeaderLossPreventsDuplication() throws IOException {
        MemberDescription member = new MemberDescription("member-0", "client-0", "host-0",
                new MemberAssignment(ImmutableSet.of(
                        new TopicPartition(SRC_TOPIC_NAME, 0),
                        new TopicPartition(SRC_TOPIC_NAME, 1))));
        Coordinator coordinator1 = new Coordinator(catalog, config, ImmutableList.of(member), clientFactory, mock(SinkTaskContext.class));
        coordinator1.start();

        TopicPartition tp = new TopicPartition(CTL_TOPIC_NAME, 0);
        consumer.rebalance(ImmutableList.of(tp));
        consumer.updateBeginningOffsets(ImmutableMap.of(tp, 0L));

        DataFile fileX = EventTestUtil.createDataFile();
        DataFile fileY = dataFile("path/to/y.parquet");

        coordinator1.process();
        UUID commit1 = lastStartCommitId();

        sendDataWritten(commit1, fileX, 1L);
        sendDataComplete(commit1, 0, 2L);  // source partition 0 — count: 1 of 2
        sendDataWritten(commit1, fileY, 3L);

        coordinator1.process();  // receivedCount=1 < 2 → no commit

        table.refresh();
        assertThat(table.snapshots()).isEmpty();

        // Rebalance: leader partition is LOST → coordinator torn down, all state discarded.
        // A new coordinator starts fresh: empty commitBuffer, receivedPartitionCount=0.
        Coordinator coordinator2 = new Coordinator(catalog, config, ImmutableList.of(member), clientFactory, mock(SinkTaskContext.class));
        coordinator2.start();
        consumer.rebalance(ImmutableList.of(tp));
        consumer.updateBeginningOffsets(ImmutableMap.of(tp, 0L));

        // Re-add all records from the beginning (MockConsumer does not replay consumed records).
        // DataComplete(commit1, p1) at offset 4 is now also present — the full transaction.
        sendDataWritten(commit1, fileX, 1L);
        sendDataComplete(commit1, 0, 2L);
        sendDataWritten(commit1, fileY, 3L);
        sendDataComplete(commit1, 1, 4L);

        // START_COMMIT(commit2) sent by coordinator2, occupies offset 5 in the control topic.
        // All four replayed records are consumed; their DataComplete ids (commit1) do not match
        // commit2, so receivedPartitionCount stays 0 — no premature commit.
        coordinator2.process();
        UUID commit2 = lastStartCommitId();

        table.refresh();
        assertThat(table.snapshots()).isEmpty();  // no premature commit

        // Worker responses for commit2 start at offset 6.
        sendDataComplete(commit2, 0, 6L);
        sendDataComplete(commit2, 1, 7L);

        coordinator2.process();
        table.refresh();

        // One clean snapshot containing both files — fileY committed exactly once.
        assertThat(table.snapshots()).hasSize(1);
        assertThat(scannedLocations()).containsEntry("path/to/y.parquet", 1L);
    }

    private Coordinator newCoordinator() {
        return newCoordinatorOn(CTL_TOPIC_NAME);
    }

    /** A coordinator on {@code controlTopic}; its dedup property key derives from that name. */
    private Coordinator newCoordinatorOn(String controlTopic) {
        SinkTaskContext context = mock(SinkTaskContext.class);
        // one member owning one source partition, so totalPartitionCount is 1 and a commit only fires
        // on a DataComplete for the CURRENT commit id. With an empty member list the count would be 0
        // and CommitState#isCommitReady would return true for any DataComplete (0 >= 0), including
        // replayed ones.
        MemberDescription member = new MemberDescription("member-0", "client-0", "host-0", new MemberAssignment(ImmutableSet.of(new TopicPartition(SRC_TOPIC_NAME, 0))));
        Coordinator coordinator = new Coordinator(catalog, config, ImmutableList.of(member), clientFactory, context);
        coordinator.start();

        TopicPartition tp = new TopicPartition(controlTopic, 0);
        consumer.rebalance(ImmutableList.of(tp));
        consumer.updateBeginningOffsets(ImmutableMap.of(tp, 0L));
        return coordinator;
    }

    private void commitCycle(Coordinator coordinator, DataFile dataFile, long dataWrittenOffset, long dataCompleteOffset) {
        commitCycleOn(CTL_TOPIC_NAME, coordinator, dataFile, dataWrittenOffset, dataCompleteOffset);
    }

    private void commitCycleOn(String controlTopic, Coordinator coordinator, DataFile dataFile, long dataWrittenOffset, long dataCompleteOffset) {
        coordinator.process();
        UUID commitId = lastStartCommitId();

        OffsetDateTime ts = EventTestUtil.now();
        addControlRecord(controlTopic, dataWrittenOffset, new Event(config.connectGroupId(), new DataWritten(StructType.of(), commitId, TableReference.of("catalog", TABLE_IDENTIFIER, null), ImmutableList.of(dataFile), ImmutableList.of())));
        addControlRecord(controlTopic, dataCompleteOffset, new Event(config.connectGroupId(), new DataComplete(commitId, ImmutableList.of(new TopicPartitionOffset("topic", 1, 1L, ts)))));

        coordinator.process();
    }

    private void addControlRecord(long offset, Event event) {
        addControlRecord(CTL_TOPIC_NAME, offset, event);
    }

    private void addControlRecord(String controlTopic, long offset, Event event) {
        consumer.addRecord(new ConsumerRecord<>(controlTopic, 0, offset, "key", AvroUtil.encode(event)));
    }

    private DataFile dataFile(String location) {
        return DataFiles.builder(table.spec()).withPath(location).withFormat(FileFormat.PARQUET).withFileSizeInBytes(100L).withRecordCount(5).build();
    }

    private UUID lastStartCommitId() {
        Event event = AvroUtil.decode(producer.history().get(producer.history().size() - 1).value());
        assertThat(event.type()).isEqualTo(PayloadType.START_COMMIT);
        return ((StartCommit) event.payload()).commitId();
    }

    private List<PayloadType> eventTypes() {
        return producer.history().stream().map(record -> AvroUtil.decode(record.value()).type()).collect(Collectors.toList());
    }

    private List<String> commitIds() {
        return Lists.newArrayList(table.snapshots()).stream().map(snapshot -> snapshot.summary().get(COMMIT_ID_SNAPSHOT_PROP)).collect(Collectors.toList());
    }

    private Map<String, Long> scannedLocations() {
        return Lists.newArrayList(table.newScan().planFiles()).stream().map(FileScanTask::file).collect(Collectors.groupingBy(file -> file.location(), Collectors.counting()));
    }

    /** Current version from the catalog pointer file — the file-system catalog's swap target. */
    private int versionHint() throws IOException {
        return Integer.parseInt(Files.readString(warehouse.resolve("db/tbl/metadata/version-hint.text")).trim());
    }

    private List<String> metadataFiles() throws IOException {
        try (Stream<Path> files = Files.list(warehouse.resolve("db/tbl/metadata"))) {
            return files.map(path -> path.getFileName().toString()).collect(Collectors.toList());
        }
    }
}
