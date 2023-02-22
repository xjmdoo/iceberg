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
import org.apache.iceberg.data.IcebergGenerics;
import org.apache.iceberg.data.Record;
import org.apache.iceberg.expressions.Expression;
import org.apache.iceberg.expressions.Expressions;
import org.apache.iceberg.expressions.UnboundPredicate;
import org.apache.iceberg.flink.FlinkSchemaUtil;
import org.apache.iceberg.flink.RowDataWrapper;
import org.apache.iceberg.flink.data.RowDataProjection;
import org.apache.iceberg.io.*;
import org.apache.iceberg.relocated.com.google.common.collect.Sets;
import org.apache.iceberg.types.TypeUtil;
import org.apache.iceberg.types.Types;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

public class PositionDeltaTaskWriter extends BaseTaskWriter<RowData>  {
    private final Table table;
    private final Schema schema;
    private final Schema deleteSchema;
    private final RowDataWrapper wrapper;
    private final RowDataWrapper keyWrapper;
    private final RowDataProjection keyProjection; // TODO ??
    private final boolean upsert;
    private final PosRowDataDeltaWriter writer;
    public PositionDeltaTaskWriter(Table table,
                                   PartitionSpec spec,
                                   PartitionKey partition,
                                   FileFormat format,
                                   FileAppenderFactory<RowData> appenderFactory,
                                   OutputFileFactory fileFactory, FileIO io,
                                   long targetFileSize,
                                   Schema schema,
                                   RowType flinkSchema,
                                   List<Integer>
                                           equalityFieldIds,
                                   boolean upsert) {
        super(spec, format, appenderFactory, fileFactory, io, targetFileSize);
        this.table = table;
        this.schema = schema;
        this.deleteSchema = TypeUtil.select(schema, Sets.newHashSet(equalityFieldIds));
        this.wrapper = new RowDataWrapper(flinkSchema, schema.asStruct());
        this.keyWrapper =
                new RowDataWrapper(FlinkSchemaUtil.convert(deleteSchema), deleteSchema.asStruct());
        this.keyProjection = RowDataProjection.create(schema, deleteSchema);
        this.upsert = upsert;
        this.writer = new PosRowDataDeltaWriter(partition);
    }

    @Override
    public void write(RowData row) throws IOException {
        switch (row.getRowKind()) {
            case INSERT:
            case UPDATE_AFTER:
                if (upsert) {
                    delete(row);
                }
                writer.write(row);
                break;

            case UPDATE_BEFORE:
                if (upsert) {
                    break; // UPDATE_BEFORE is not necessary for UPSERT, we do nothing to prevent delete one
                    // row twice
                }
                delete(row);
                break;
            case DELETE:
                delete(row);
                break;

            default:
                throw new UnsupportedOperationException("Unknown row kind: " + row.getRowKind());
        }
    }

    private void delete(RowData row) throws IOException {
        List<Types.NestedField> columns = new ArrayList<>(schema.columns());
        columns.add(MetadataColumns.ROW_POSITION);
        columns.add(MetadataColumns.FILE_PATH);
        StructLike wrappedRow = wrapper.wrap(row);
        List<UnboundPredicate<?>> predicates = Lists.newArrayList();
        for (int i = 0; i < deleteSchema.columns().size(); i++) {
            Types.NestedField field = deleteSchema.columns().get(i);
            predicates.add(Expressions.equal(field.name(), wrappedRow.get(i, field.type().typeId().javaClass())));
        }
        Expression expr = Expressions.alwaysTrue();
        for (UnboundPredicate<?> predicate : predicates) {
            expr = Expressions.and(expr, predicate);
        }

        Schema projectedSchema = new Schema(columns);
        try (CloseableIterable<Record> result = IcebergGenerics
                .read(table)
                .project(projectedSchema)
                .where(expr)
                .build()) {

            for (Record rec : result) {
                System.out.println(rec);
                CharSequence filePath = (CharSequence) rec.getField(MetadataColumns.FILE_PATH.name());
                long rowPos = (long) rec.getField(MetadataColumns.ROW_POSITION.name());
                writer.delete(filePath, rowPos);
            }
        }
    }

    @Override
    public void close() throws IOException {
        // TODO
    }

    protected  class PosRowDataDeltaWriter extends BasePositionDeltaWriter {

        public PosRowDataDeltaWriter(PartitionKey partition) {
            super(partition, schema, deleteSchema);
        }

        @Override
        protected StructLike asStructLike(RowData data) {
            return wrapper.wrap(data);
        }

        @Override
        protected StructLike asStructLikeKey(RowData data) {
            return keyWrapper.wrap(data);
        }
    }
}
