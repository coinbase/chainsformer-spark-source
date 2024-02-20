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

import org.apache.arrow.flight.CallOptions;
import org.apache.arrow.flight.FlightClient;
import org.apache.arrow.flight.FlightStream;
import org.apache.arrow.flight.Ticket;
import org.apache.arrow.flight.spark.datasource.ClientConfig;
import org.apache.arrow.flight.spark.datasource.DataSourceOptions;
import org.apache.arrow.util.AutoCloseables;
import org.apache.spark.broadcast.Broadcast;
import org.apache.spark.sql.connector.read.PartitionReader;
import org.apache.spark.sql.vectorized.ColumnVector;
import org.apache.spark.sql.vectorized.ColumnarBatch;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.concurrent.TimeUnit;

public class FlightInputPartitionReader implements PartitionReader<ColumnarBatch> {
    private static final Logger logger = LoggerFactory.getLogger(FlightInputPartitionReader.class);
    private final FlightClient client;
    private final FlightStream stream;
    private final FlightClientFactory clientFactory;

    public FlightInputPartitionReader(Broadcast<DataSourceOptions> options, byte[] ticketBytes) {
        Ticket ticket = new Ticket(ticketBytes);
        ClientConfig clientConfig = options.getValue().getClientConfig();
        logger.info("setting up a data reader at host {} and port {} with ticket {}", clientConfig.getHost(), clientConfig.getPort(), new String(ticket.getBytes()));

        this.clientFactory = new FlightClientFactory(clientConfig);
        this.client = clientFactory.apply();
        this.stream = client.getStream(ticket, CallOptions.timeout(10, TimeUnit.MINUTES));
    }

    @Override
    public boolean next() throws IOException {
        try {
            return stream.next();
        } catch (Throwable t) {
            throw new IOException(t);
        }
    }

    @Override
    public ColumnarBatch get() {
        ColumnarBatch batch = new ColumnarBatch(
                stream.getRoot().getFieldVectors()
                        .stream()
                        .map(FlightArrowColumnVector::new)
                        .toArray(ColumnVector[]::new)
        );
        batch.setNumRows(stream.getRoot().getRowCount());
        return batch;
    }

    @Override
    public void close() throws IOException {
        try {
            AutoCloseables.close(stream, client, clientFactory);
        } catch (Exception e) {
            throw new IOException(e);
        }
    }
}
