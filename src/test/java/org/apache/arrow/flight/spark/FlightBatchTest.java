package org.apache.arrow.flight.spark;

import org.apache.arrow.flight.Ticket;
import org.apache.arrow.flight.FlightDescriptor;
import org.apache.arrow.flight.FlightInfo;
import org.apache.arrow.flight.FlightClient;
import org.apache.arrow.flight.FlightEndpoint;
import org.apache.arrow.flight.spark.datasource.ClientConfig;
import org.apache.arrow.flight.spark.datasource.DataSourceOptions;
import org.apache.arrow.flight.spark.datasource.StreamQuery;
import org.apache.arrow.vector.types.Types;
import org.apache.arrow.vector.types.pojo.Field;
import org.apache.arrow.vector.types.pojo.Schema;
import org.apache.spark.broadcast.Broadcast;
import org.apache.spark.sql.connector.read.InputPartition;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.Mock;
import org.mockito.junit.MockitoJUnitRunner;

import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.stream.Collectors;

import static org.junit.Assert.assertArrayEquals;
import static org.junit.Assert.assertEquals;
import static org.mockito.Mockito.when;

@RunWith(MockitoJUnitRunner.class)
public class FlightBatchTest {
    private final int DEFAULT_TAG = 0;
    private final long DEFAULT_EVENTS_START_OFFSET = 0;
    private final long DEFAULT_EVENTS_END_OFFSET = 310;
    private final int DEFAULT_EVENTS_PER_PARTITION = 100;
    private final int DEFAULT_EVENTS_PER_RECORD = 10;
    private final int DEFAULT_EVENTS_MAX_BATCH_SIZE = 300;
    private final int DEFAULT_PARTITION_BY_SIZE = 100000;
    private final String DEFAULT_COMPRESSION = "";
    private final String DEFAULT_TABLE_NAME = "streamed_transactions";
    private final String DEFAULT_FORMAT = "native";
    private final String DEFAULT_ENCODING = "none";

    private FlightBatch flightBatch;
    private StreamQuery query;

    @Mock
    private FlightClientFactory clientFactory;

    @Mock
    private FlightClient client;

    @Mock
    Broadcast<DataSourceOptions> broadcastDataSourceOptions;

    @Before
    public void setup() {
        ClientConfig clientConfig = new ClientConfig("local",
                9090,
                "username",
                "password",
                false,
                false,
                "unit_test",
                "unit_test",
                3,
                "ethereum",
                "mainnet",
                "dev");

        query = new StreamQuery(DEFAULT_TAG,
                DEFAULT_EVENTS_START_OFFSET,
                DEFAULT_EVENTS_END_OFFSET,
                DEFAULT_EVENTS_PER_PARTITION,
                DEFAULT_EVENTS_PER_RECORD,
                DEFAULT_EVENTS_MAX_BATCH_SIZE,
                DEFAULT_PARTITION_BY_SIZE,
                DEFAULT_COMPRESSION,
                DEFAULT_TABLE_NAME,
                DEFAULT_FORMAT,
                DEFAULT_ENCODING);


        flightBatch = new FlightBatch(broadcastDataSourceOptions , clientFactory);
        DataSourceOptions datasourceOptions = new DataSourceOptions(clientConfig, query);

        when(clientFactory.apply()).thenReturn(client);
        when(broadcastDataSourceOptions.value()).thenReturn(datasourceOptions);
    }

    @Test
    public void planInputPartitions_ReturnsThreeBatches_When_ExecutedSuccessfullyWithoutRetry() {
        List<String> queries = query.getBatchQueries();
        Ticket ticket1 = new Ticket("ticket1".getBytes());
        Ticket ticket2 = new Ticket("ticket2".getBytes());
        Ticket ticket3 = new Ticket("ticket3".getBytes());
        Ticket ticket4 = new Ticket("ticket4".getBytes());

        when(client.getInfo(FlightDescriptor.command(queries.get(0).getBytes()))).thenReturn(
                new FlightInfo(
                        new Schema(Collections.singletonList(Field.nullable("field1", Types.MinorType.FLOAT8.getType()))),
                        FlightDescriptor.command(queries.get(0).getBytes()),
                        Arrays.asList(new FlightEndpoint(ticket1), new FlightEndpoint(ticket2)),
                        10,
                        20)
        );
        when(client.getInfo(FlightDescriptor.command(queries.get(1).getBytes()))).thenReturn(
                new FlightInfo(
                        new Schema(Collections.singletonList(Field.nullable("field1", Types.MinorType.FLOAT8.getType()))),
                        FlightDescriptor.command(queries.get(0).getBytes()),
                        Arrays.asList(new FlightEndpoint(ticket3), new FlightEndpoint(ticket4)),
                        10,
                        20)
        );

        List<InputPartition> actualPartitions = Arrays.asList(flightBatch.planInputPartitions());
        List<InputPartition> expectedPartitions = Arrays.asList(
                new FlightInputPartition(ticket1.getBytes()),
                new FlightInputPartition(ticket2.getBytes()),
                new FlightInputPartition(ticket3.getBytes()),
                new FlightInputPartition(ticket4.getBytes()));

        List<String[]> expected = expectedPartitions.stream().map(InputPartition::preferredLocations).collect(Collectors.toList());
        List<String[]> actual = actualPartitions.stream().map(InputPartition::preferredLocations).collect(Collectors.toList());

        assertEquals(expected.size(), actual.size());
        for(int idx = 0; idx < expected.size(); ++idx) {
            assertArrayEquals(expected.get(idx), actual.get(idx));
        }
    }
}
