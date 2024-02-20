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
import org.apache.arrow.flight.spark.datasource.MetricsClient;
import org.apache.spark.broadcast.Broadcast;
import org.apache.spark.sql.connector.read.Batch;
import org.apache.spark.sql.connector.read.Scan;
import org.apache.spark.sql.connector.read.streaming.MicroBatchStream;
import org.apache.spark.sql.types.StructType;

public class FlightScan implements Scan {
    private final StructType schema;
    private final FlightClientFactory clientFactory;
    private final Broadcast<DataSourceOptions> dataSourceOptions;
    private final String sql;
    private final MetricsClient metricsClient;

    public FlightScan(StructType schema, Broadcast<DataSourceOptions> dataSourceOptions, FlightClientFactory clientFactory, String sql) {
        this.schema = schema;
        this.dataSourceOptions = dataSourceOptions;
        this.clientFactory = clientFactory;
        this.sql = sql;
        this.metricsClient = new MetricsClient(dataSourceOptions.value().getClientConfig());
    }

    @Override
    public StructType readSchema() {
        return schema;
    }

    @Override
    public String description() {
        return "flight_scan";
    }

    @Override
    public Batch toBatch() {
        return new FlightBatch(dataSourceOptions, clientFactory);
    }

    @Override
    public MicroBatchStream toMicroBatchStream(String checkpointLocation) {
        return new FlightMicroBatch(dataSourceOptions, clientFactory, metricsClient);
    }
}
