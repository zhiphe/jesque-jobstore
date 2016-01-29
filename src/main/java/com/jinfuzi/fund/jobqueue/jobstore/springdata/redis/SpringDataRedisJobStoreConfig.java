package com.jinfuzi.fund.jobqueue.jobstore.springdata.redis;

import com.jinfuzi.fund.jobqueue.jobstore.JobStoreConfig;

/**
 * Created by kevinhe on 16/1/29.
 */
public class SpringDataRedisJobStoreConfig implements JobStoreConfig {
    private String host;
    private int port;
    private String namespace;
    private int database;

    public SpringDataRedisJobStoreConfig(String host, int port, String namespace, int database) {
        this.host = host;
        this.port = port;
        this.namespace = namespace;
        this.database = database;
    }

    public String getHost() {
        return host;
    }

    public void setHost(String host) {
        this.host = host;
    }

    public int getPort() {
        return port;
    }

    public void setPort(int port) {
        this.port = port;
    }

    public String getNamespace() {
        return namespace;
    }

    public void setNamespace(String namespace) {
        this.namespace = namespace;
    }

    public int getDatabase() {
        return database;
    }

    public void setDatabase(int database) {
        this.database = database;
    }
}
