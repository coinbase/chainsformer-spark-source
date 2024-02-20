package org.apache.arrow.flight.spark.datasource;

import org.apache.arrow.flight.Location;
import java.io.Serializable;

public class ClientConfig implements Serializable {
    private static final String ENV_PRODUCTION = "production";
    private static final String ENV_DEVELOPMENT = "development";

    private final String host;
    private final int port;
    private final String username;
    private final String password;
    private final boolean parallel;
    private final boolean useTls;
    private final String projectName;
    private final String jobName;
    private final String blockchain;
    private final String network;
    private final String env;
    private final int tier;

    public ClientConfig(final String host,
                        final int port,
                        final String username,
                        final String password,
                        final boolean parallel,
                        final boolean useTls,
                        final String projectName,
                        final String jobName,
                        final int tier,
                        final String blockchain,
                        final String network,
                        final String env) {
        this.host = host;
        this.port = port;
        this.username = username;
        this.password = password;
        this.parallel = parallel;
        this.useTls = useTls;
        this.projectName = projectName == null ? null : projectName.toLowerCase();
        this.jobName = jobName == null ? null : jobName.toLowerCase();
        this.tier = tier;
        this.blockchain = blockchain == null ? null : blockchain.toLowerCase();
        this.network = network == null ? null : network.toLowerCase();
        this.env = env == null ? null : parseEnv(env);
    }

    public Location getLocation() {
        return Location.forGrpcInsecure(getHost(), getPort());
    }

    public String getHost() {
        return this.host;
    }

    public int getPort() {
        return this.port;
    }

    public String getUsername() {
        return this.username;
    }

    public String getPassword() {
        return this.password;
    }

    public boolean isParallel() {
        return this.parallel;
    }

    public boolean isUseTls() {
        return this.useTls;
    }

    public String getProjectName() {
        return this.projectName;
    }

    public String getJobName() {
        return this.jobName;
    }

    public int getTier() {
        return this.tier;
    }

    public String getBlockchain() {
        return this.blockchain;
    }

    public String getNetwork() {
        return this.network;
    }

    public String getEnv() {
        return this.env;
    }

    private String parseEnv(String env) {
        switch (env.toLowerCase()) {
            case "prod":
            case "production":
                return ENV_PRODUCTION;
            case "dev":
            case "development":
                return ENV_DEVELOPMENT;
            default:
                throw new IllegalArgumentException(String.format("unexpected env: %s", this.env));
        }
    }
}
