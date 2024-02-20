package org.apache.arrow.flight.spark.datasource;

import com.google.common.collect.ImmutableMap;
import org.apache.arrow.flight.Location;
import org.apache.commons.lang3.StringUtils;

import java.io.Serializable;
import java.util.Locale;
import java.util.Map;

public class ClientConfig implements Serializable {
    private static final String ENV_PRODUCTION = "production";
    private static final String ENV_DEVELOPMENT = "development";
    private static final Map<String, String> hostByEnv = ImmutableMap.of(
            ENV_PRODUCTION, "data-chainsformer-%s-%s.cbhq.net",
            ENV_DEVELOPMENT, "data-chainsformer-%s-%s-dev.cbhq.net"
    );

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
    private final String clientID;

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
                        final String env,
                        final String clientID) {
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
        this.clientID = clientID == null? null: clientID.toLowerCase();
    }

    public Location getLocation() {
        return Location.forGrpcInsecure(getHost(), getPort());
    }

    public String getHost() {
        if (StringUtils.isNotBlank(this.host)) {
            return this.host;
        }
        if (StringUtils.isBlank(this.blockchain) || StringUtils.isBlank(this.network) || StringUtils.isBlank(this.env)) {
            return this.host;
        }
        return String.format(hostByEnv.get(this.env), this.blockchain, this.network);
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

    public String getClientID() { return this.clientID;}

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
