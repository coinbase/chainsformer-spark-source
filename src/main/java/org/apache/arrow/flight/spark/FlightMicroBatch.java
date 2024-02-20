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

import org.apache.arrow.flight.Action;
import org.apache.arrow.flight.FlightClient;
import org.apache.arrow.flight.FlightDescriptor;
import org.apache.arrow.flight.FlightInfo;
import org.apache.arrow.flight.Result;
import org.apache.arrow.flight.spark.datasource.DataSourceOptions;
import org.apache.arrow.flight.spark.datasource.MetricsClient;
import org.apache.spark.broadcast.Broadcast;
import org.apache.spark.sql.connector.read.InputPartition;
import org.apache.spark.sql.connector.read.PartitionReaderFactory;
import org.apache.spark.sql.connector.read.streaming.MicroBatchStream;
import org.apache.spark.sql.connector.read.streaming.Offset;
import org.apache.spark.sql.connector.read.streaming.ReadLimit;
import org.apache.spark.sql.connector.read.streaming.SupportsAdmissionControl;
import org.apache.spark.sql.execution.streaming.LongOffset;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.retry.RetryContext;
import org.springframework.retry.support.RetryTemplate;

import java.util.Iterator;

public class FlightMicroBatch implements SupportsAdmissionControl, MicroBatchStream {
    private static final Logger LOGGER = LoggerFactory.getLogger(FlightBatch.class);
    private final FlightClientFactory clientFactory;
    private final Broadcast<DataSourceOptions> dataSourceOptions;
    private final RetryTemplate retryTemplate;
    private final MetricsClient metricsClient;

    public FlightMicroBatch(Broadcast<DataSourceOptions> dataSourceOptions, FlightClientFactory clientFactory, MetricsClient metricsClient) {
        this.dataSourceOptions = dataSourceOptions;
        this.clientFactory = clientFactory;
        this.retryTemplate = new RetryTemplateFactory().apply();
        this.metricsClient = metricsClient;
    }

    @Override
    public Offset latestOffset() {
        throw new UnsupportedOperationException("latestOffset() not supported");
    }

    @Override
    public InputPartition[] planInputPartitions(Offset start, Offset end) {
        return retryTemplate.execute(context -> planInputPartitionsOnce(context, start, end));
    }

    private InputPartition[] planInputPartitionsOnce(RetryContext context, Offset start, Offset end) {
        LOGGER.info(String.format("Attempting to plan input partitions for the %d time", context.getRetryCount()));
        try (FlightClient client = clientFactory.apply()) {
            FlightInfo info = client.getInfo(FlightDescriptor.command(dataSourceOptions.getValue().getQuery().getMicroBatchQuery(start, end).getBytes()));
            InputPartition[] batches = info.getEndpoints()
                    .stream()
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

    @Override
    public Offset initialOffset() {
        long startOffset = retryTemplate.execute(this::getStartOffset);
        return new LongOffset(Math.max(startOffset, dataSourceOptions.value().getQuery().getStartPosition()));
    }

    @Override
    public Offset deserializeOffset(String s) {
        return new LongOffset(Long.parseLong(s));
    }

    @Override
    public void commit(Offset offset) {
    }

    @Override
    public void stop() {
        metricsClient.close();
    }

    @Override
    public ReadLimit getDefaultReadLimit() {
        long limit = dataSourceOptions.value().getQuery().getMaxBatchSize();
        if (limit != 0) {
            return ReadLimit.maxRows(limit);
        }
        return SupportsAdmissionControl.super.getDefaultReadLimit();
    }

    @Override
    public Offset reportLatestOffset() {
        long tipOffset = retryTemplate.execute(this::getTipOffset);
        return new LongOffset(tipOffset);
    }

    @Override
    public Offset latestOffset(Offset offset, ReadLimit readLimit) {
        return retryTemplate.execute(context -> getLatestOffsetOnce(context, offset));
    }

    public Offset getLatestOffsetOnce(RetryContext context, Offset offset) {
        long tipOffset = getTipOffset(context);
        long limit = dataSourceOptions.value().getQuery().getMaxBatchSize();

        // This metric measures the offset of the spark job.
        LongOffset startOffset = (LongOffset) offset;
        metricsClient.gauge("micro_batch.offset", startOffset.offset());
        LOGGER.info("micro_batch.offset: {}", startOffset.offset());

        // This metric measures how far the spark job is falling behind chainsformer.
        long offsetDelta = tipOffset - startOffset.offset();
        metricsClient.gauge("micro_batch.offset_delta", offsetDelta);
        LOGGER.info("micro_batch.offset_delta: {}", offsetDelta);

        if (limit != 0 && offsetDelta > limit) {
            return new LongOffset(startOffset.offset() + limit);
        }

        metricsClient.gauge("micro_batch.offset_delta", offsetDelta);
        LOGGER.info("micro_batch.offset_delta: {}", offsetDelta);

        return new LongOffset(Math.max(tipOffset, startOffset.offset()));
    }

    private long getTipOffset(RetryContext context) {
        LOGGER.info(String.format("Attempting to get latest offset for the %d time", context.getRetryCount()));
        return getOffset(dataSourceOptions.getValue().getQuery().getTipAction());
    }

    private long getStartOffset(RetryContext context) {
        LOGGER.info(String.format("Attempting to get start offset for the %d time", context.getRetryCount()));
        return getOffset(dataSourceOptions.getValue().getQuery().getStartAction());
    }

    private long getOffset(Action action) {
        try (FlightClient client = clientFactory.apply()) {
            Iterator<Result> res = client.doAction(action);
            long offset = 0;
            if (res.hasNext()) {
                offset = Long.parseLong(new String(res.next().getBody()));
            }
            return offset;
        } catch (InterruptedException e) {
            throw new RuntimeException(e);
        }
    }
}
