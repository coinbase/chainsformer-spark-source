package org.apache.arrow.flight.spark.datasource;

import com.google.common.collect.ImmutableMap;

import java.util.Map;

public class DataSourceOptionsProvider {
    enum JobType {
        BATCH,
        STREAM
    }

    private static final Map<String, JobType> tableToJobType = ImmutableMap.of(
            "transactions", JobType.BATCH,
            "blocks", JobType.BATCH,
            "streamed_transactions", JobType.STREAM,
            "streamed_blocks", JobType.STREAM
    );

    public static DataSourceOptions getDataSourceOptions(Map<String, String> properties) {
        String sql = properties.getOrDefault("path", "");
        Query query;
        JobType jobType = tableToJobType.getOrDefault(sql, JobType.BATCH);
        if (jobType == JobType.STREAM) {
            query = new StreamQuery(
                    Integer.parseInt(properties.getOrDefault("tag", "0")),
                    Long.parseLong(properties.getOrDefault("events_start_offset", "0")),
                    Long.parseLong(properties.getOrDefault("events_end_offset", "0")),
                    Integer.parseInt(properties.getOrDefault("events_per_partition", "0")),
                    Integer.parseInt(properties.getOrDefault("events_per_record", "0")),
                    Integer.parseInt(properties.getOrDefault("events_max_batch_size", "0")),
                    Integer.parseInt(properties.getOrDefault("partition_by_size", "0")),
                    properties.getOrDefault("compression", null),
                    sql,
                    properties.getOrDefault("format", "native"),
                    properties.getOrDefault("encoding", "none"));
        } else {
            query = new BatchQuery(
                    Integer.parseInt(properties.getOrDefault("tag", "0")),
                    Long.parseLong(properties.getOrDefault("blocks_start_offset", "0")),
                    Long.parseLong(properties.getOrDefault("blocks_end_offset", "0")),
                    Integer.parseInt(properties.getOrDefault("blocks_per_partition", "0")),
                    Integer.parseInt(properties.getOrDefault("blocks_per_record", "0")),
                    Integer.parseInt(properties.getOrDefault("blocks_max_batch_size", "0")),
                    Integer.parseInt(properties.getOrDefault("partition_by_size", "0")),
                    properties.getOrDefault("compression", null),
                    sql,
                    properties.getOrDefault("format", "native"),
                    properties.getOrDefault("encoding", "none"));
        }

        return new DataSourceOptions(
                new ClientConfig(
                        properties.getOrDefault("host", ""),
                        Integer.parseInt(properties.getOrDefault("port", "9090")),
                        properties.getOrDefault("username", "anonymous"),
                        properties.getOrDefault("password", null),
                        Boolean.parseBoolean(properties.getOrDefault("parallel", "false")),
                        Boolean.parseBoolean(properties.getOrDefault("useTls", "true")),
                        properties.getOrDefault("project_name", null),
                        properties.getOrDefault("job_name", null),
                        Integer.parseInt(properties.getOrDefault("tier", "0")),
                        properties.getOrDefault("blockchain", null),
                        properties.getOrDefault("network", null),
                        properties.getOrDefault("env", null),
                        properties.getOrDefault("client_id", null)
                ),
                query);
    }
}
