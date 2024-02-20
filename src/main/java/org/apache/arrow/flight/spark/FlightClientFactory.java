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

import io.grpc.ClientInterceptor;
import io.grpc.ManagedChannel;
import io.grpc.netty.NettyChannelBuilder;
import io.netty.channel.ChannelOption;
import org.apache.arrow.flight.FlightClient;
import org.apache.arrow.flight.FlightGrpcUtils;
import org.apache.arrow.flight.Location;
import org.apache.arrow.flight.LocationSchemes;
import org.apache.arrow.flight.spark.datasource.ClientConfig;
import org.apache.arrow.flight.spark.datasource.ClientIDInterceptor;
import org.apache.arrow.memory.BufferAllocator;
import org.apache.arrow.memory.RootAllocator;

public class FlightClientFactory implements AutoCloseable {
    // The maximum number of trace events to keep on the gRPC Channel. This value disables channel tracing.
    private static final int MAX_CHANNEL_TRACE_EVENTS = 0;

    // The maximum size of an individual gRPC message. This effectively disables the limit.
    private static final int MAX_INBOUND_MESSAGE_SIZE = Integer.MAX_VALUE;

    // The socket connection timeout.
    private static final int CONNECT_TIMEOUT_MILLIS = 10_000;

    private final BufferAllocator allocator = new RootAllocator();
    private final Location defaultLocation;
    private final boolean useTls;
    private final String clientID;

    public FlightClientFactory(ClientConfig config) {
        this.defaultLocation = config.getLocation();
        this.useTls = config.isUseTls();
        this.clientID = config.getClientID();
    }

    public FlightClient apply() {
        // The implementation is mostly the same as FlightClient.Builder.build(), except that
        // CONNECT_TIMEOUT_MILLIS is overridden to work around sporadic connection timeouts.
        //
        // TODO: We may pool the connections by storing ManagedChannel at the factory level
        // and creating FlightClient via FlightGrpcUtils.createFlightClientWithSharedChannel().
        NettyChannelBuilder channelBuilder = NettyChannelBuilder
                .forAddress(defaultLocation.getUri().getHost(), defaultLocation.getUri().getPort())
                .maxTraceEvents(MAX_CHANNEL_TRACE_EVENTS)
                .maxInboundMessageSize(MAX_INBOUND_MESSAGE_SIZE)
                .withOption(ChannelOption.CONNECT_TIMEOUT_MILLIS, CONNECT_TIMEOUT_MILLIS);
        if (useTls || LocationSchemes.GRPC_TLS.equals(defaultLocation.getUri().getScheme())) {
            channelBuilder.useTransportSecurity();
        } else {
            channelBuilder.usePlaintext();
        }
        if (clientID != null) {
            channelBuilder.intercept(new ClientIDInterceptor(clientID));
        }

        ManagedChannel channel = channelBuilder.build();
        return FlightGrpcUtils.createFlightClient(allocator, channel);
    }

    @Override
    public void close() {
        allocator.close();
    }
}
