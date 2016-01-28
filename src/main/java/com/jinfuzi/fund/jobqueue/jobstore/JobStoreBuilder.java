package com.jinfuzi.fund.jobqueue.jobstore;

import net.greghaines.jesque.Config;
import redis.clients.jedis.Jedis;

/**
 * Created by kevinhe on 16/1/28.
 */
public final class JobStoreBuilder {
    public static final JobStore buildJobStore(Config config) {
        if (config instanceof JedisJobStoreConfig) {
            return buildJedisJobStore((JedisJobStoreConfig) config);
        }
        return null;
    }

    protected static final JedisJobStoreImpl buildJedisJobStore(JedisJobStoreConfig config) {
        Jedis jedis = new Jedis(config.getHost(), config.getPort());
        return new JedisJobStoreImpl(jedis);
    }
}
