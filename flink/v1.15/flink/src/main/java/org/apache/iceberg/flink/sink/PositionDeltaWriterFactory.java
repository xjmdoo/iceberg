/*
 *
 *  * Licensed to the Apache Software Foundation (ASF) under one
 *  * or more contributor license agreements.  See the NOTICE file
 *  * distributed with this work for additional information
 *  * regarding copyright ownership.  The ASF licenses this file
 *  * to you under the Apache License, Version 2.0 (the
 *  * "License"); you may not use this file except in compliance
 *  * with the License.  You may obtain a copy of the License at
 *  *
 *  *   http://www.apache.org/licenses/LICENSE-2.0
 *  *
 *  * Unless required by applicable law or agreed to in writing,
 *  * software distributed under the License is distributed on an
 *  * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 *  * KIND, either express or implied.  See the License for the
 *  * specific language governing permissions and limitations
 *  * under the License.
 *
 */

package org.apache.iceberg.flink.sink;

import org.apache.commons.compress.utils.Lists;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.types.logical.RowType;
import org.apache.iceberg.*;
import org.apache.iceberg.deletes.PositionDelete;
import org.apache.iceberg.flink.RowDataWrapper;
import org.apache.iceberg.io.*;
import org.apache.iceberg.relocated.com.google.common.base.Preconditions;
import org.apache.iceberg.relocated.com.google.common.collect.Iterables;
import org.apache.iceberg.util.CharSequenceSet;
import org.apache.iceberg.util.Tasks;
import org.apache.iceberg.util.ThreadPools;

import java.io.IOException;
import java.util.List;

public class PositionDeltaWriterFactory implements TaskWriterFactory<RowData> {
    private final Table table;
    private final RowType flinkSchema;
    private final long targetFileSizeInBytes;
    private final FlinkFileWriterFactory writerFactory;
    private OutputFileFactory outputFileFactory;

    public PositionDeltaWriterFactory(Table table, RowType flinkSchema, long targetFileSizeInBytes) {
        this.table = table;
        this.flinkSchema = flinkSchema;
        this.targetFileSizeInBytes = targetFileSizeInBytes;
        // TODO: need to build the object with correct configs
        this.writerFactory = FlinkFileWriterFactory
                .builderFor(table)
                .dataSchema(table.schema())
                .dataFlinkType(flinkSchema)
                .build();
    }

    @Override
    public void initialize(int taskId, int attemptId) {
        // TODO: need to build the object with correct configs
        outputFileFactory = OutputFileFactory.builderFor(table, taskId, attemptId).build();
    }

    @Override
    public TaskWriter<RowData> create() {
        Preconditions.checkNotNull(
                outputFileFactory,
                "The outputFileFactory shouldn't be null if we have invoked the initialize().");

        return new NewFancyDeltaWriter(table, flinkSchema, writerFactory, outputFileFactory, targetFileSizeInBytes);
    }

    public static class NewFancyDeltaWriter implements TaskWriter<RowData> {

        private final Table table;
        private final BasePositionDeltaWriter<RowData> writer;

        public NewFancyDeltaWriter(Table table, RowType flinkSchema, FlinkFileWriterFactory fileWriterFactory, OutputFileFactory outputFileFactory, long targetFileSizeInBytes) {
            this.table = table;

            this.writer = new BasePositionDeltaWriter<>(
                    new FanoutDataWriter<>(fileWriterFactory, outputFileFactory, table.io(), targetFileSizeInBytes),
                    new FanoutDataWriter<>(fileWriterFactory, outputFileFactory, table.io(), targetFileSizeInBytes),
                    new FanoutPositionDeleteWriter<>(fileWriterFactory, outputFileFactory, table.io(), targetFileSizeInBytes));
        }

        @Override
        public void write(RowData row) throws IOException {
            writer.insert(row, PartitionSpec.unpartitioned(), null);
        }

        @Override
        public void abort() throws IOException {
            // TODO impl
        }

        @Override
        public WriteResult complete() throws IOException {
            // TODO check this
            close();
            return writer.result();
        }

        @Override
        public void close() throws IOException {
            // TODO check this
            writer.close();
        }
    }
}
