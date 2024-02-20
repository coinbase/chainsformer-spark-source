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

import org.apache.arrow.flight.FlightClient;
import org.apache.arrow.flight.SchemaResult;
import org.apache.arrow.flight.spark.datasource.DataSourceOptions;
import org.apache.arrow.flight.spark.datasource.DataSourceOptionsProvider;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.broadcast.Broadcast;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.connector.catalog.SupportsRead;
import org.apache.spark.sql.connector.catalog.TableCapability;
import org.apache.spark.sql.connector.expressions.Transform;
import org.apache.spark.sql.connector.read.ScanBuilder;
import org.apache.spark.sql.execution.arrow.FlightArrowUtils;
import org.apache.spark.sql.types.Metadata;
import org.apache.spark.sql.types.StructField;
import org.apache.spark.sql.types.StructType;
import org.apache.spark.sql.util.CaseInsensitiveStringMap;
import org.springframework.retry.RetryContext;
import org.springframework.retry.support.RetryTemplate;

import java.util.HashSet;
import java.util.Map;
import java.util.Set;

public class FlightTable implements SupportsRead {
    private SparkSession lazySpark;
    private JavaSparkContext lazySparkContext;

    private StructType schema;
    private final Transform[] partitioning;
    private final Map<String, String> properties;
    private Set<TableCapability> capabilities;
    private final Broadcast<DataSourceOptions> dataSourceOptions;

    public FlightTable(StructType schema, Transform[] partitioning, Map<String, String> properties) {
        this.schema = schema;
        this.partitioning = partitioning;
        this.properties = properties;

        DataSourceOptions options = DataSourceOptionsProvider.getDataSourceOptions(properties);
        this.dataSourceOptions = lazySparkContext().broadcast(options);
    }

    @Override
    public ScanBuilder newScanBuilder(CaseInsensitiveStringMap options) {
        return new FlightScanBuilder(dataSourceOptions);
    }

    @Override
    public Transform[] partitioning() {
        return partitioning;
    }

    @Override
    public Map<String, String> properties() {
        return properties;
    }

    @Override
    public String name() {
        return "dummy_table";
    }

    @Override
    public Set<TableCapability> capabilities() {
        if (capabilities == null) {
            this.capabilities = new HashSet<>();
            capabilities.add(TableCapability.BATCH_READ);
            capabilities.add(TableCapability.MICRO_BATCH_READ);
        }
        return capabilities;
    }

    @Override
    public StructType schema() {
        if (schema == null) {
            schema = readSchemaImpl();
        }
        return schema;
    }

    private StructType readSchemaImpl() {
        try (FlightScanBuilder builder = new FlightScanBuilder(dataSourceOptions)) {
            return builder.readSchema();
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }

    private SparkSession lazySparkSession() {
        if (lazySpark == null) {
            this.lazySpark = SparkSession.builder().getOrCreate();
        }
        return lazySpark;
    }

    private JavaSparkContext lazySparkContext() {
        if (lazySparkContext == null) {
            this.lazySparkContext = new JavaSparkContext(lazySparkSession().sparkContext());
        }
        return lazySparkContext;
    }
}
