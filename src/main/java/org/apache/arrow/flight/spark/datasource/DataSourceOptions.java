package org.apache.arrow.flight.spark.datasource;

import java.io.Serializable;
import java.util.StringJoiner;

public class DataSourceOptions implements Serializable {
    private final ClientConfig clientConfig;
    private final Query query;

    public DataSourceOptions(final ClientConfig clientConfig, final Query query) {
        this.clientConfig = clientConfig;
        this.query = query;
    }

    public ClientConfig getClientConfig() {
        return this.clientConfig;
    }

    public Query getQuery() {
        return this.query;
    }

    @Override
    public String toString() {
        return new StringJoiner(", ", DataSourceOptions.class.getSimpleName() + "[", "]")
                .add("host='" + clientConfig.getHost() + "'")
                .add("port=" + clientConfig.getPort())
                .add("sql='" + query.getSql() + "'")
                .add("username='" + clientConfig.getUsername() + "'")
                .add("password='" + clientConfig.getPassword() + "'")
                .add("parallel=" + clientConfig.isParallel())
                .add("useTls=" + clientConfig.isUseTls())
                .toString();
    }
}
