package org.apache.arrow.flight.spark.datasource;

import org.apache.arrow.flight.Action;
import org.apache.arrow.flight.FlightDescriptor;
import org.apache.spark.sql.execution.streaming.LongOffset;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.junit.MockitoJUnitRunner;

import java.util.Arrays;
import java.util.Collections;
import java.util.List;

import static org.junit.Assert.assertEquals;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.core.Is.is;
import static org.junit.Assert.assertTrue;

@RunWith(MockitoJUnitRunner.class)
public class StreamQueryTest {
    private final int DEFAULT_TAG = 0;
    private final long DEFAULT_EVENTS_START_OFFSET = 5;
    private final long DEFAULT_EVENTS_END_OFFSET = 500;
    private final int DEFAULT_EVENTS_PER_PARTITION = 100;
    private final int DEFAULT_EVENTS_PER_RECORD = 10;
    private final int DEFAULT_EVENTS_MAX_BATCH_SIZE = 20000;
    private final int DEFAULT_PARTITION_BY_SIZE = 100000;
    private final String DEFAULT_COMPRESSION = "";
    private final String DEFAULT_TABLE_NAME = "streamed_transactions";
    private final String DEFAULT_FORMAT = "native";
    private final String DEFAULT_ENCODING = "none";

    private Query defaultQuery;

    @Before
    public void setup(){
        defaultQuery = createStreamQuery(DEFAULT_EVENTS_START_OFFSET, DEFAULT_EVENTS_END_OFFSET);
    }

    @Test
    public void getMicroBatchQuery_Should_ReturnCorrectQuery_When_WithDefaultParams() {
        long startSequence = 1;
        long batchSize = 99;

        assertEquals(getGenericStreamQuery(startSequence, startSequence + batchSize), defaultQuery.getMicroBatchQuery(new LongOffset(startSequence), new LongOffset(startSequence + batchSize)));
    }

    @Test
    public void getBatchQueries_Should_ReturnOneQuery_When_RequestedSmallerBatchThanMax() {
        long startSequence = 1;
        long batchSize = 99;

        StreamQuery query = createStreamQuery(startSequence, startSequence + batchSize);
        List<String> expectedQueries = Collections.singletonList(getGenericStreamQuery(startSequence, startSequence + batchSize));

        assertEquals(expectedQueries, query.getBatchQueries());
    }

    @Test
    public void getBatchQueries_Should_ReturnOneQuery_When_RequestedEqualBatchAsMax() {
        long startSequence = 1;
        long batchSize = DEFAULT_EVENTS_MAX_BATCH_SIZE;

        StreamQuery query = createStreamQuery(startSequence, startSequence + batchSize);
        List<String> expectedQueries = Collections.singletonList(getGenericStreamQuery(startSequence, startSequence + batchSize));

        assertEquals(expectedQueries, query.getBatchQueries());
    }

    @Test
    public void getBatchQueries_Should_ReturnTwoQueries_When_RequestedLargerBatchThanMax() {
        long startSequence = 1;
        long batchSize = 20200;

        StreamQuery query = createStreamQuery(startSequence, startSequence + batchSize);
        List<String> expectedQueries = Arrays.asList(
                getGenericStreamQuery(startSequence, startSequence + DEFAULT_EVENTS_MAX_BATCH_SIZE),
                getGenericStreamQuery(startSequence + DEFAULT_EVENTS_MAX_BATCH_SIZE, startSequence + batchSize));

        assertEquals(expectedQueries, query.getBatchQueries());
    }

    @Test
    public void getBatchQueries_Should_ReturnTwoQueries_When_RequestedTwiceBatchAsMax() {
        long startSequence = 1;
        long batchSize = 40000;

        StreamQuery query = createStreamQuery(startSequence, startSequence + batchSize);
        List<String> expectedQueries = Arrays.asList(
                getGenericStreamQuery(startSequence, startSequence + DEFAULT_EVENTS_MAX_BATCH_SIZE),
                getGenericStreamQuery(startSequence + DEFAULT_EVENTS_MAX_BATCH_SIZE, startSequence + batchSize));

        assertEquals(expectedQueries, query.getBatchQueries());
    }

    @Test
    public void getStartPosition_Should_ReturnCorrectValue_When_SetToFive() {
        assertEquals(DEFAULT_EVENTS_START_OFFSET, defaultQuery.getStartPosition());
    }

    @Test
    public void getMaxBatchSize_Should_ReturnCorrectValue_When_SetTo20000() {
        assertEquals(DEFAULT_EVENTS_MAX_BATCH_SIZE, defaultQuery.getMaxBatchSize());
    }

    @Test
    public void getStartAction_Should_ReturnCorrectActionObject_When_TagIsZero() {
        Action expectedAction = new Action("STREAM_EARLIEST", String.format("{\"event_tag\": %d}", DEFAULT_TAG).getBytes());

        assertEquals(expectedAction.getType(), defaultQuery.getStartAction().getType());
        assertThat(expectedAction.getBody(), is(defaultQuery.getStartAction().getBody()));
    }

    @Test
    public void getTipAction_Should_ReturnCorrectActionObject_When_TagIsZero() {
        Action expectedAction = new Action("STREAM_TIP", String.format("{\"event_tag\": %d}", DEFAULT_TAG).getBytes());

        assertEquals(expectedAction.getType(), defaultQuery.getTipAction().getType());
        assertThat(expectedAction.getBody(), is(defaultQuery.getTipAction().getBody()));
    }

    @Test
    public void getSchemaDescriptor_Should_ReturnCorrectSchemaDescriptor_When_WithDefaultParams() {
        String cmd = String.format("{\"table\": \"%s\", \"format\": \"%s\", \"encoding\": \"%s\"}", DEFAULT_TABLE_NAME, DEFAULT_FORMAT, DEFAULT_ENCODING);
        FlightDescriptor expectedFlightDescriptor = FlightDescriptor.command(cmd.getBytes());

        assertTrue(defaultQuery.getSchemaDescriptor().isCommand());
        assertThat(expectedFlightDescriptor.getCommand(), is(defaultQuery.getSchemaDescriptor().getCommand()));
    }

    private StreamQuery createStreamQuery(long startOffset, long endOffset) {
        return new StreamQuery(
                DEFAULT_TAG,
                startOffset,
                endOffset,
                DEFAULT_EVENTS_PER_PARTITION,
                DEFAULT_EVENTS_PER_RECORD,
                DEFAULT_EVENTS_MAX_BATCH_SIZE,
                DEFAULT_PARTITION_BY_SIZE,
                DEFAULT_COMPRESSION,
                DEFAULT_TABLE_NAME,
                DEFAULT_FORMAT,
                DEFAULT_ENCODING
        );
    }

    private String getGenericStreamQuery(long startOffset, long endOffset) {
        return String.format("{\"stream_query\": {\"tag\": %d, \"start_sequence\": %d, \"end_sequence\": %d, \"events_per_partition\": %d, \"events_per_record\": %d, \"partition_by_size\": %d, \"compression\": \"%s\", \"table\": \"%s\", \"format\": \"%s\", \"encoding\": \"%s\"}}",
                DEFAULT_TAG,
                startOffset,
                endOffset,
                DEFAULT_EVENTS_PER_PARTITION,
                DEFAULT_EVENTS_PER_RECORD,
                DEFAULT_PARTITION_BY_SIZE,
                DEFAULT_COMPRESSION,
                DEFAULT_TABLE_NAME,
                DEFAULT_FORMAT,
                DEFAULT_ENCODING);
    }
}
