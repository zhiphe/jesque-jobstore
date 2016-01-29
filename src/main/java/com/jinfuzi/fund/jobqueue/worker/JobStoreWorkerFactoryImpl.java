package com.jinfuzi.fund.jobqueue.worker;

import com.jinfuzi.fund.jobqueue.jobstore.JobStoreConfig;
import net.greghaines.jesque.worker.JobFactory;

import java.util.Collection;
import java.util.concurrent.Callable;

/**
 * Created by kevinhe on 16/1/29.
 */
public class JobStoreWorkerFactoryImpl implements Callable<JobStoreWorkerImpl> {
    private final JobStoreConfig jobStoreConfig;
    private final Collection<String> queues;
    private final JobFactory jobFactory;

    public JobStoreWorkerFactoryImpl(JobStoreConfig jobStoreConfig, Collection<String> queues, JobFactory jobFactory) {
        this.jobStoreConfig = jobStoreConfig;
        this.queues = queues;
        this.jobFactory = jobFactory;
    }

    public JobStoreWorkerImpl call() {
        return new JobStoreWorkerImpl(this.jobStoreConfig, this.queues, this.jobFactory);
    }

}
