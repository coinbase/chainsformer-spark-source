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
import org.apache.arrow.flight.FlightDescriptor;
import org.apache.arrow.flight.spark.datasource.DataSourceOptions;
import org.apache.spark.broadcast.Broadcast;
import org.apache.spark.sql.connector.read.Batch;
import org.apache.spark.sql.connector.read.InputPartition;
import org.apache.spark.sql.connector.read.PartitionReaderFactory;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.retry.RetryContext;
import org.springframework.retry.support.RetryTemplate;

import java.util.Arrays;
import java.util.List;

public class FlightBatch implements Batch {
    private static final Logger LOGGER = LoggerFactory.getLogger(FlightBatch.class);
    private final FlightClientFactory clientFactory;
    private final Broadcast<DataSourceOptions> dataSourceOptions;
    private final RetryTemplate retryTemplate;

    public FlightBatch(Broadcast<DataSourceOptions> dataSourceOptions, FlightClientFactory clientFactory) {
        this.dataSourceOptions = dataSourceOptions;
        this.clientFactory = clientFactory;
        this.retryTemplate = new RetryTemplateFactory().apply();
    }

    @Override
    public InputPartition[] planInputPartitions() {
        return retryTemplate.execute(this::planInputPartitionsOnce);
    }

    private InputPartition[] planInputPartitionsOnce(RetryContext context) {
        LOGGER.info(String.format("Attempting to plan input partitions for the %d time", context.getRetryCount()));
        try (FlightClient client = clientFactory.apply()) {
            List<String> queries = dataSourceOptions.value().getQuery().getBatchQueries();
            InputPartition[] batches = queries.stream()
                    .map(String::getBytes)
                    .map(FlightDescriptor::command)
                    .map(client::getInfo)
                    .flatMap(info -> info.getEndpoints().stream())
                    .map(endpoint -> new FlightInputPartition(endpoint.getTicket().getBytes()))
                    .toArray(FlightInputPartition[]::new);
            LOGGER.info("Created {} batches from arrow endpoints", batches.length);
            return batches;
        } catch (InterruptedException e) {
            throw new RuntimeException(e);
        }
    }

    @Override
    public PartitionReaderFactory createReaderFactory() {
        return new FlightInputPartitionReaderFactory(dataSourceOptions);
    }
}
