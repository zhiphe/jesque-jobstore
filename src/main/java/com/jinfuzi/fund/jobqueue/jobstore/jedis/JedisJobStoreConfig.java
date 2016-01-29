package com.jinfuzi.fund.jobqueue.jobstore.jedis;

import com.jinfuzi.fund.jobqueue.jobstore.JobStoreConfig;
import net.greghaines.jesque.Config;

import java.util.Set;

/**
 * Created by kevinhe on 16/1/28.
 */
public class JedisJobStoreConfig implements JobStoreConfig {
    private final String namespace;
    private final String password;
    private final int database;

    private final String host;
    private final int port;

    public JedisJobStoreConfig(String host, int port, String namespace, String password, int database) {
        this.host = host;
        this.port = port;
        this.namespace = namespace;
        this.password = password;
        this.database = database;
    }

    public String getNamespace() {
        return namespace;
    }

    public String getPassword() {
        return password;
    }

    public int getDatabase() {
        return database;
    }

    public int getPort() {
        return port;
    }

    public String getHost() {
        return host;
    }
}
