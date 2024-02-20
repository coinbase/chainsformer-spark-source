package org.apache.arrow.flight.spark.datasource;

import org.apache.arrow.flight.Action;
import org.apache.arrow.flight.FlightDescriptor;
import org.apache.spark.sql.connector.read.streaming.Offset;
import org.apache.spark.sql.execution.streaming.LongOffset;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.List;

public class StreamQuery implements Query, Serializable {
    private final int tag;
    private final long eventStartOffset;
    private final long eventEndOffset;
    private final int eventsPerPartition;
    private final int eventsPerRecord;
    private final int eventsMaxBatchSize;
    private final int partitionBySize;
    private final String compression;
    private final String sql;
    private final String format;
    private final String encoding;

    public StreamQuery(final int tag,
                      final long eventStartOffset,
                      final long eventEndOffset,
                      final int eventsPerPartition,
                      final int eventsPerRecord,
                      final int eventsMaxBatchSize,
                      final int partitionBySize,
                      final String compression,
                      final String sql,
                      final String format,
                      final String encoding) {
        this.tag = tag;
        this.eventStartOffset = eventStartOffset;
        this.eventEndOffset = eventEndOffset;
        this.eventsPerPartition = eventsPerPartition;
        this.eventsPerRecord = eventsPerRecord;
        this.eventsMaxBatchSize = eventsMaxBatchSize;
        this.partitionBySize = partitionBySize;
        this.compression = compression;
        this.sql = sql;
        this.format = format;
        this.encoding = encoding;
    }

    @Override
    public String getSql() {
        return this.sql;
    }

    @Override
    public String getMicroBatchQuery(Offset start, Offset end) {
        return String.format(
                "{\"stream_query\": {\"tag\": %d, \"start_sequence\": %d, \"end_sequence\": %d, \"events_per_partition\": %d, \"events_per_record\": %d, \"partition_by_size\": %d, \"compression\": \"%s\", \"table\": \"%s\", \"format\": \"%s\", \"encoding\": \"%s\"}}",
                tag,
                ((LongOffset)start).offset(),
                ((LongOffset)end).offset(),
                eventsPerPartition,
                eventsPerRecord,
                partitionBySize,
                compression,
                sql,
                format,
                encoding);
    }

    @Override
    public List<String> getBatchQueries() {
        List<String> queries = new ArrayList<>();

        // Break the [eventStartOffset, eventEndOffset) range into batches of no more than eventsMaxBatchSize.
        // In each batch get [start, end)
        long start = eventStartOffset;
        do {
            long end = Math.min(start + eventsMaxBatchSize, eventEndOffset);
            queries.add(
                String.format(
                    "{\"stream_query\": {\"tag\": %d, \"start_sequence\": %d, \"end_sequence\": %d, \"events_per_partition\": %d, \"events_per_record\": %d, \"partition_by_size\": %d, \"compression\": \"%s\", \"table\": \"%s\", \"format\": \"%s\", \"encoding\": \"%s\"}}",
                    tag,
                    start,
                    end,
                    eventsPerPartition,
                    eventsPerRecord,
                    partitionBySize,
                    compression,
                    sql,
                    format,
                    encoding));

            // end is exclusive, no +1 here
            start = end;
        } while (start < eventEndOffset);

        return queries;
    }

    @Override
    public long getStartPosition() {
        return this.eventStartOffset;
    }

    @Override
    public int getMaxBatchSize() {
        return this.eventsMaxBatchSize;
    }

    @Override
    public Action getTipAction() {
        return new Action("STREAM_TIP", String.format("{\"event_tag\": %d}", this.tag).getBytes());
    }

    @Override
    public Action getStartAction() {
        return new Action("STREAM_EARLIEST", String.format("{\"event_tag\": %d}", this.tag).getBytes());
    }

    @Override
    public FlightDescriptor getSchemaDescriptor() {
        String cmd = String.format("{\"table\": \"%s\", \"format\": \"%s\", \"encoding\": \"%s\"}", this.sql, this.format, this.encoding);
        return FlightDescriptor.command(cmd.getBytes());
    }
}
