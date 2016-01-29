package com.jinfuzi.fund.jobqueue.jobstore.jedis;

import com.jinfuzi.fund.jobqueue.jobstore.JobStore;
import com.jinfuzi.fund.jobqueue.jobstore.JobStoreBuilder;
import com.jinfuzi.fund.jobqueue.jobstore.JobStoreConfig;
import net.greghaines.jesque.Config;
import redis.clients.jedis.Jedis;

/**
 * Created by kevinhe on 16/1/28.
 */
public final class JedisJobStoreBuilder implements JobStoreBuilder {

    @Override
    public final JobStore buildJobStore(JobStoreConfig jobStoreConfig) {
        return new JedisJobStoreImpl((JedisJobStoreConfig) jobStoreConfig);
    }
}
