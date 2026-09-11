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
package org.apache.iceberg.connect.data;

import static org.assertj.core.api.Assertions.assertThat;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.List;
import org.apache.hadoop.conf.Configuration;
import org.apache.iceberg.DataFile;
import org.apache.iceberg.DataFiles;
import org.apache.iceberg.FileFormat;
import org.apache.iceberg.FileScanTask;
import org.apache.iceberg.ManifestFile;
import org.apache.iceberg.ManifestFiles;
import org.apache.iceberg.ManifestReader;
import org.apache.iceberg.Schema;
import org.apache.iceberg.Snapshot;
import org.apache.iceberg.Table;
import org.apache.iceberg.catalog.TableIdentifier;
import org.apache.iceberg.data.GenericAppenderFactory;
import org.apache.iceberg.data.GenericRecord;
import org.apache.iceberg.data.IcebergGenerics;
import org.apache.iceberg.data.Record;
import org.apache.iceberg.hadoop.HadoopCatalog;
import org.apache.iceberg.io.CloseableIterable;
import org.apache.iceberg.io.FileAppender;
import org.apache.iceberg.io.OutputFile;
import org.apache.iceberg.relocated.com.google.common.collect.Lists;
import org.apache.iceberg.types.Types;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

/**
 * What "duplication" means for an Iceberg table: the same data file location referenced by more
 * than one live manifest entry.
 *
 * <p>The spec requires that this never happens. Quoting the "Scan Planning" section: "for any
 * snapshot, all file paths marked with 'ADDED' or 'EXISTING' may appear at most once across all
 * manifest files in the snapshot. If a file path appears more than once, the results of the scan
 * are undefined. Reader implementations may raise an error in this case, but are not required to do
 * so."
 *
 * <p>It is a writer-side invariant though, and nothing in the library enforces it: appending an
 * already-committed file succeeds silently, as {@link #sameFileInTwoCommitsIsSpecInvalid()} shows.
 * Only the in-commit case is guarded, see {@link #sameFileInOneCommitIsDeduped()}.
 *
 * <p>The first two tests are metadata-only and their parquet paths never exist; {@link
 * #duplicateEntriesAreReadTwice()} writes real files so the duplication can be observed through a
 * reader.
 */
class TestDuplicated {

  private static final String FILE_A = "data/file-1.parquet";

  private HadoopCatalog catalog;
  private Table table;

  @BeforeEach
  void setup() throws IOException {
    Path warehouse = Files.createTempDirectory("iceberg-test");
    catalog = new HadoopCatalog(new Configuration(), warehouse.toString());

    Schema schema =
        new Schema(
            Types.NestedField.required(1, "id", Types.LongType.get()),
            Types.NestedField.required(2, "data", Types.StringType.get()));

    table = catalog.createTable(TableIdentifier.of("db", "events"), schema);
  }

  @AfterEach
  void tearDown() throws IOException {
    catalog.close();
  }

  /**
   * Across commits nothing dedups, so the append succeeds and leaves the table in a state the spec
   * calls invalid: one path, two live entries, scan results undefined. The assertions below record
   * what this implementation happens to do with such a table (count it twice, read it twice), not
   * behaviour anyone should rely on.
   */
  @Test
  void sameFileInTwoCommitsIsSpecInvalid() throws IOException {
    DataFile fileA = dataFile(FILE_A);

    table.newAppend().appendFile(fileA).commit();
    table.newAppend().appendFile(fileA).commit();

    assertThat(Lists.newArrayList(table.snapshots())).hasSize(2);

    // two live manifest entries with the same location: the spec violation itself
    assertThat(liveLocations(table.currentSnapshot())).containsExactly(FILE_A, FILE_A);

    // the 10 physical rows are counted twice in the snapshot summary
    assertThat(table.currentSnapshot().summary())
        .containsEntry("added-data-files", "1")
        .containsEntry("added-records", "10")
        .containsEntry("total-data-files", "2")
        .containsEntry("total-records", "20");

    // and scan planning hands the same file to the reader twice
    List<FileScanTask> tasks = Lists.newArrayList(table.newScan().planFiles());
    assertThat(tasks).hasSize(2);
    assertThat(tasks).allSatisfy(task -> assertThat(task.file().location()).isEqualTo(FILE_A));
  }

  /**
   * Within a single commit Iceberg does dedup, keyed on {@link DataFile#location()} by the {@code
   * DataFileSet} in {@code MergingSnapshotProducer#add}.
   */
  @Test
  void sameFileInOneCommitIsDeduped() throws IOException {
    DataFile fileA = dataFile(FILE_A);

    table.newAppend().appendFile(fileA).appendFile(fileA).commit();

    assertThat(Lists.newArrayList(table.snapshots())).hasSize(1);
    assertThat(liveLocations(table.currentSnapshot())).containsExactly(FILE_A);
    assertThat(table.currentSnapshot().summary())
        .containsEntry("total-data-files", "1")
        .containsEntry("total-records", "10");
    assertThat(Lists.newArrayList(table.newScan().planFiles())).hasSize(1);
  }

  /**
   * The same thing seen through an actual Iceberg reader instead of the metadata: writes 10 real
   * rows to one parquet file, appends that file in two commits, and reads the table back with
   * {@link IcebergGenerics}. 20 records come out of 10 stored ones.
   */
  @Test
  void duplicateEntriesAreReadTwice() throws IOException {
    DataFile fileA = writeRecords("data/real-1.parquet", 10);

    table.newAppend().appendFile(fileA).commit();
    table.newAppend().appendFile(fileA).commit();

    List<Record> records = Lists.newArrayList();
    try (CloseableIterable<Record> reader = IcebergGenerics.read(table).build()) {
      reader.forEach(records::add);
    }

    assertThat(records).hasSize(20);
    assertThat(records.stream().map(record -> record.getField("id")).distinct()).hasSize(10);
  }

  /** Writes {@code count} rows as one real parquet file and describes it as a {@link DataFile}. */
  private DataFile writeRecords(String name, int count) throws IOException {
    OutputFile out = table.io().newOutputFile(table.location() + "/" + name);
    GenericAppenderFactory appenders = new GenericAppenderFactory(table.schema(), table.spec());

    try (FileAppender<Record> appender = appenders.newAppender(out, FileFormat.PARQUET)) {
      for (int i = 0; i < count; i += 1) {
        GenericRecord record = GenericRecord.create(table.schema());
        record.setField("id", (long) i);
        record.setField("data", "row-" + i);
        appender.add(record);
      }
      appender.close();

      return DataFiles.builder(table.spec())
          .withPath(out.location())
          .withFormat(FileFormat.PARQUET)
          .withFileSizeInBytes(appender.length())
          .withMetrics(appender.metrics())
          .build();
    }
  }

  private DataFile dataFile(String location) {
    return DataFiles.builder(table.spec())
        .withPath(location)
        .withFileSizeInBytes(100)
        .withRecordCount(10)
        .build();
  }

  private List<String> liveLocations(Snapshot snapshot) throws IOException {
    List<String> locations = Lists.newArrayList();
    for (ManifestFile manifest : snapshot.allManifests(table.io())) {
      try (ManifestReader<DataFile> reader =
          ManifestFiles.read(manifest, table.io(), table.specs())) {
        for (DataFile file : reader) {
          locations.add(file.location());
        }
      }
    }
    return locations;
  }
}
