package com.jinfuzi.fund.jobqueue.jobstore;

/**
 * Created by kevinhe on 16/1/29.
 */
public interface JobProcessorFactory {
    JobProcessor getJobProcessor(String type, String jobJson) throws Exception;
}
