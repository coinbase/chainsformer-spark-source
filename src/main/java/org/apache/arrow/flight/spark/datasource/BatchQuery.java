package org.apache.arrow.flight.spark.datasource;

import org.apache.arrow.flight.Action;
import org.apache.arrow.flight.FlightDescriptor;
import org.apache.spark.sql.connector.read.streaming.Offset;
import org.apache.spark.sql.execution.streaming.LongOffset;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.List;

public class BatchQuery implements Query, Serializable {
    private final int tag;
    private final long blockStartOffset;
    private final long blockEndOffset;
    private final int blocksPerPartition;
    private final int blocksPerRecord;
    private final int blocksMaxBatchSize;
    private final int partitionBySize;
    private final String compression;
    private final String sql;
    private final String format;
    private final String encoding;

    public BatchQuery(final int tag,
                      final long blockStartOffset,
                      final long blockEndOffset,
                      final int blocksPerPartition,
                      final int blocksPerRecord,
                      final int blocksMaxBatchSize,
                      final int partitionBySize,
                      final String compression,
                      final String sql,
                      final String format,
                      final String encoding) {
        this.tag = tag;
        this.blockStartOffset = blockStartOffset;
        this.blockEndOffset = blockEndOffset;
        this.blocksPerPartition = blocksPerPartition;
        this.blocksPerRecord = blocksPerRecord;
        this.blocksMaxBatchSize = blocksMaxBatchSize;
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
                "{\"batch_query\": {\"tag\": %d, \"start_height\": %d, \"end_height\": %d, \"blocks_per_partition\": %d, \"blocks_per_record\": %d, \"partition_by_size\": %d, \"compression\": \"%s\", \"table\": \"%s\", \"format\": \"%s\", \"encoding\": \"%s\"}}",
                tag,
                ((LongOffset)start).offset(),
                ((LongOffset)end).offset(),
                blocksPerPartition,
                blocksPerRecord,
                partitionBySize,
                compression,
                sql,
                format,
                encoding);
    }

    @Override
    public List<String> getBatchQueries() {
        List<String> queries = new ArrayList<>();

        // Break the [blockStartOffset, blockEndOffset) range into batches of no more than blocksMaxBatchSize.
        // In each batch get [start, end)
        long start = blockStartOffset;
        do {
            long end = Math.min(start + blocksMaxBatchSize, blockEndOffset);
            queries.add(
                String.format(
                    "{\"batch_query\": {\"tag\": %d, \"start_height\": %d, \"end_height\": %d, \"blocks_per_partition\": %d, \"blocks_per_record\": %d, \"partition_by_size\": %d, \"compression\": \"%s\", \"table\": \"%s\", \"format\": \"%s\", \"encoding\": \"%s\"}}",
                    tag,
                    start,
                    end,
                    blocksPerPartition,
                    blocksPerRecord,
                    partitionBySize,
                    compression,
                    sql,
                    format,
                    encoding)
            );
            // end is exclusive, no +1 here
            start = end;
        } while (start < blockEndOffset);

        return queries;
    }

    @Override
    public long getStartPosition() {
        return this.blockStartOffset;
    }

    @Override
    public int getMaxBatchSize() {
        return this.blocksMaxBatchSize;
    }

    @Override
    public Action getTipAction() {
        return new Action("TIP");
    }

    @Override
    public Action getStartAction() {
        return new Action("EARLIEST");
    }

    @Override
    public FlightDescriptor getSchemaDescriptor() {
        String cmd = String.format("{\"table\": \"%s\", \"format\": \"%s\", \"encoding\": \"%s\"}", this.sql, this.format, this.encoding);
        return FlightDescriptor.command(cmd.getBytes());
    }
}
