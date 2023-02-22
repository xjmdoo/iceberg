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
package org.apache.iceberg.flink.sink;

import java.io.IOException;
import java.io.UncheckedIOException;
import java.util.List;
import java.util.Map;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.types.logical.RowType;
import org.apache.iceberg.*;
import org.apache.iceberg.flink.RowDataWrapper;
import org.apache.iceberg.io.*;
import org.apache.iceberg.relocated.com.google.common.collect.Maps;
import org.apache.iceberg.util.Tasks;

// TODO: Create a base class for this which implements common methods with the unpratitioned writer
class PartitionedDeltaWriter implements TaskWriter<RowData> {

  private final Table table;
  private final PartitionSpec spec;
  private final FileFormat format;
  private final FileAppenderFactory<RowData> appenderFactory;
  private final OutputFileFactory fileFactory;
  private final FileIO io;
  private final long targetFileSize;
  private final Schema schema;
  private final RowType flinkSchema;
  private final List<Integer> equalityFieldIds;
  private final boolean upsert;
  private final PartitionKey partitionKey;
  private final boolean usePosDelete;
  private final RowDataWrapper wrapper;

  private final Map<PartitionKey, TaskWriter<RowData>> writers = Maps.newHashMap();

  PartitionedDeltaWriter(
          Table table,
      PartitionSpec spec,
      FileFormat format,
      FileAppenderFactory<RowData> appenderFactory,
      OutputFileFactory fileFactory,
      FileIO io,
      long targetFileSize,
      Schema schema,
      RowType flinkSchema,
      List<Integer> equalityFieldIds,
      boolean upsert,
      boolean usePosDelete) {
    this.table = table;
    this.spec = spec;
    this.format = format;
    this.appenderFactory = appenderFactory;
    this.fileFactory = fileFactory;
    this.io = io;
    this.targetFileSize = targetFileSize;
    this.schema = schema;
    this.flinkSchema = flinkSchema;
    this.equalityFieldIds = equalityFieldIds;
    this.upsert = upsert;
    this.partitionKey = new PartitionKey(spec, schema);
    this.usePosDelete = usePosDelete;

    this.wrapper = new RowDataWrapper(flinkSchema, schema.asStruct());
  }

  private TaskWriter<RowData> createWriter(PartitionKey partition) {
    return usePosDelete ? new PositionDeltaTaskWriter(table, spec, partition, format, appenderFactory, fileFactory, io, targetFileSize, schema, flinkSchema, equalityFieldIds, upsert) :
            new BaseDeltaTaskWriter(spec, partition, format, appenderFactory, fileFactory, io, targetFileSize, schema, flinkSchema, equalityFieldIds, upsert);
  }

  public void write(RowData row) throws IOException {
    TaskWriter<RowData> writer = route(row);
    writer.write(row);
  }

  TaskWriter<RowData> route(RowData row) {
    partitionKey.partition(this.wrapper.wrap(row));

    TaskWriter<RowData> writer = writers.get(partitionKey);
    if (writer == null) {
      // NOTICE: we need to copy a new partition key here, in case of messing up the keys in
      // writers.
      PartitionKey copiedKey = partitionKey.copy();
      writer = createWriter(copiedKey);
      writers.put(copiedKey, writer);
    }

    return writer;
  }

  @Override
  public WriteResult complete() throws IOException {
    return null; // TODO
  }

  @Override
  public void abort() throws IOException {
    // TODO
  }

  @Override
  public void close() {
    try {
      Tasks.foreach(writers.values())
          .throwFailureWhenFinished()
          .noRetry()
          .run(TaskWriter::close, IOException.class);

      writers.clear();
    } catch (IOException e) {
      throw new UncheckedIOException("Failed to close equality delta writer", e);
    }
  }
}
