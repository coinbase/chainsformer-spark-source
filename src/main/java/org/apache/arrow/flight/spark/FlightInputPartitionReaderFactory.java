/*
 * Copyright (C) 2019 The flight-spark-source Authors
 * Copyright (C) 2024 Coinbase Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.arrow.flight.spark;

import org.apache.arrow.flight.spark.datasource.DataSourceOptions;
import org.apache.spark.broadcast.Broadcast;
import org.apache.spark.sql.catalyst.InternalRow;
import org.apache.spark.sql.connector.read.InputPartition;
import org.apache.spark.sql.connector.read.PartitionReader;
import org.apache.spark.sql.connector.read.PartitionReaderFactory;
import org.apache.spark.sql.vectorized.ColumnarBatch;

public class FlightInputPartitionReaderFactory implements PartitionReaderFactory {
    private final Broadcast<DataSourceOptions> options;

    public FlightInputPartitionReaderFactory(Broadcast<DataSourceOptions> options) {
        this.options = options;
    }

    @Override
    public boolean supportColumnarReads(InputPartition partition) {
        return true;
    }

    @Override
    public PartitionReader<ColumnarBatch> createColumnarReader(InputPartition partition) {
        return new FlightInputPartitionReader(options, partition.preferredLocations()[0].getBytes());
    }

    @Override
    public PartitionReader<InternalRow> createReader(InputPartition partition) {
        throw new UnsupportedOperationException("Cannot create internal row reader.");
    }
}
