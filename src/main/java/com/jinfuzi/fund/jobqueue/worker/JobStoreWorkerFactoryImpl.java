package com.jinfuzi.fund.jobqueue.worker;

import com.jinfuzi.fund.jobqueue.jobstore.JobProcessorFactory;
import com.jinfuzi.fund.jobqueue.jobstore.JobStoreConfig;

import java.util.Collection;
import java.util.concurrent.Callable;

/**
 * Created by kevinhe on 16/1/29.
 */
public class JobStoreWorkerFactoryImpl implements Callable<JobStoreWorkerImpl> {
    private final JobStoreConfig jobStoreConfig;
    private final Collection<String> queues;
    private final JobProcessorFactory jobProcessorFactory;

    public JobStoreWorkerFactoryImpl(JobStoreConfig jobStoreConfig, Collection<String> queues, JobProcessorFactory jobProcessorFactory) {
        this.jobStoreConfig = jobStoreConfig;
        this.queues = queues;
        this.jobProcessorFactory = jobProcessorFactory;
    }

    public JobStoreWorkerImpl call() {
        return new JobStoreWorkerImpl(this.jobStoreConfig, this.queues, this.jobProcessorFactory);
    }

}
