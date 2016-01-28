package com.jinfuzi.fund.jobqueue.jobstore;

import net.greghaines.jesque.Config;

import java.util.Set;

/**
 * Created by kevinhe on 16/1/28.
 */
public class JedisJobStoreConfig extends Config {
    public JedisJobStoreConfig(Config config) {
        super(config.getHost(), config.getPort(), config.getTimeout(), config.getPassword(), config.getNamespace(), config.getDatabase());
    }

    public JedisJobStoreConfig(String host, int port, int timeout, String password, String namespace, int database) {
        super(host, port, timeout, password, namespace, database);
    }

    public JedisJobStoreConfig(Set<String> sentinels, String masterName, int timeout, String password, String namespace, int database) {
        super(sentinels, masterName, timeout, password, namespace, database);
    }
}
