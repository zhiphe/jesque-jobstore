package com.jinfuzi.fund.jobqueue.jobstore;

import net.greghaines.jesque.json.ObjectMapperFactory;

import java.io.IOException;

/**
 * Created by kevinhe on 16/1/29.
 */
public abstract class JobProcessor implements Runnable {
    private Job job;
    
    public JobProcessor(String jobJson) throws IOException {
        this.job = deserializeJob(jobJson);
    }
    
    protected Job deserializeJob(String jobJson) throws IOException {
        return ObjectMapperFactory.get().readValue(jobJson, getJobType());
    }
    
    public abstract JobResult processJob(Job job);
    public abstract Class<? extends Job> getJobType();

    public Job getJob() {
        return job;
    }

    @Override
    public void run() {
        processJob(this.job);
    }
}
