package com.jinfuzi.fund.longregression;

import com.jinfuzi.fund.jobqueue.jobstore.Job;
import com.jinfuzi.fund.jobqueue.jobstore.JobProcessor;
import com.jinfuzi.fund.jobqueue.jobstore.JobResult;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;

/**
 * Created by kevinhe on 16/1/29.
 */
public class TestJobProcessor extends JobProcessor {
    private Logger logger = LoggerFactory.getLogger(TestJobProcessor.class);

    public TestJobProcessor(String jobJson) throws IOException {
        super(jobJson);
    }

    @Override
    public JobResult processJob(Job job) {
        logger.info(((TestJob) job).getMessage());
        return null;
    }

    @Override
    public Class<? extends Job> getJobType() {
        return TestJob.class;
    }
}
