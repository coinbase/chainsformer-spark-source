package org.apache.arrow.flight.spark.datasource;

import com.timgroup.statsd.NoOpStatsDClient;
import com.timgroup.statsd.NonBlockingStatsDClientBuilder;
import com.timgroup.statsd.StatsDClient;
import org.apache.commons.lang3.StringUtils;
import org.apache.commons.lang3.tuple.ImmutablePair;

import java.util.Arrays;
import java.util.Collections;
import java.util.stream.Stream;

public class MetricsClient implements AutoCloseable {
    private final StatsDClient statsdClient;

    public MetricsClient(ClientConfig clientConfig) {
        String projectName = clientConfig.getProjectName();
        String jobName = clientConfig.getJobName();
        int tier = clientConfig.getTier();
        String blockchain = clientConfig.getBlockchain();
        String network = clientConfig.getNetwork();
        String env = clientConfig.getEnv();
        if (StringUtils.isBlank(projectName)
                || StringUtils.isBlank(jobName)
                || StringUtils.isBlank(blockchain)
                || StringUtils.isBlank(network)) {
            statsdClient = new NoOpStatsDClient();
        } else {
            statsdClient = new NonBlockingStatsDClientBuilder()
                    .prefix("flight_spark_source")
                    .hostname(System.getenv().getOrDefault("DD_AGENT_HOST", "localhost"))
                    .port(Integer.parseInt(System.getenv().getOrDefault("DD_DOGSTATSD_PORT", "8125")))
                    .constantTags(
                            formatTag("projectname", projectName),
                            formatTag("jobname", jobName),
                            formatTag("configname", String.format("%s-%s", blockchain, network)),
                            formatTag("tier", Integer.toString(tier)),
                            formatTag("blockchain", blockchain),
                            formatTag("network", network),
                            formatTag("env", env)
                    )
                    .build();
        }
    }

    @Override
    public void close() {
        statsdClient.stop();
    }

    @SafeVarargs
    public final void count(String metric, int delta, ImmutablePair<String, String>... tags) {
        statsdClient.count(metric, delta, formatTags(tags));
    }

    @SafeVarargs
    public final void increment(String metric, ImmutablePair<String, String>... tags) {
        statsdClient.increment(metric, formatTags(tags));
    }

    @SafeVarargs
    public final void gauge(String metric, long value, ImmutablePair<String, String>... tags) {
        statsdClient.gauge(metric, value, formatTags(tags));
    }

    @SafeVarargs
    public final void gauge(String metric, double value, ImmutablePair<String, String>... tags) {
        statsdClient.gauge(metric, value, formatTags(tags));
    }

    @SafeVarargs
    public final void time(String metric, long timeInMs, ImmutablePair<String, String>... tags) {
        statsdClient.time(metric, timeInMs, formatTags(tags));
    }

    private String[] formatTags(ImmutablePair<String, String>[] tags) {
        return Arrays.stream(tags)
                .map(pair -> formatTag(pair.getKey(), pair.getValue()))
                .toArray(String[]::new);
    }

    private String formatTag(String key, String value) {
        return String.format("%s:%s", key, value);
    }
}
