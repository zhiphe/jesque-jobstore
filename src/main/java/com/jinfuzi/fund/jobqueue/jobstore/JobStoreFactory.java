package com.jinfuzi.fund.jobqueue.jobstore;

import com.jinfuzi.fund.jobqueue.jobstore.jedis.JedisJobStoreBuilder;
import com.jinfuzi.fund.jobqueue.jobstore.jedis.JedisJobStoreConfig;

import java.util.HashMap;
import java.util.Map;

/**
 * Created by kevinhe on 16/1/28.
 */
public class JobStoreFactory {
    public static final Map<String, JobStoreBuilder> jobStoreBuilders = new HashMap<>();

    static {
        jobStoreBuilders.put(JedisJobStoreConfig.class.getName(), new JedisJobStoreBuilder());
    }

    public static void addJobStoreBuilder(JobStoreConfig jobStoreConfig, JobStoreBuilder jobStoreBuilder) {
        jobStoreBuilders.put(jobStoreConfig.getClass().getName(), jobStoreBuilder);
    }

    public static final JobStore buildJobStore(JobStoreConfig jobStoreConfig) {
        JobStoreBuilder jobStoreBuilder = jobStoreBuilders.get(jobStoreConfig.getClass().getName());
        if (jobStoreBuilder != null) {
            return jobStoreBuilder.buildJobStore(jobStoreConfig);
        } else {
            throw new RuntimeException("not supported config \"" + jobStoreConfig.getClass() + "\"");
        }
    }
}
