package com.jinfuzi.fund.jobqueue.jobstore;

/**
 * Created by kevinhe on 16/1/28.
 */
public interface JobStoreBuilder {
    JobStore buildJobStore(JobStoreConfig jobStoreConfig);
}
