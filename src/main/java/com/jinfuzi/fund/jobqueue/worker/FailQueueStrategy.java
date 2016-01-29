package com.jinfuzi.fund.jobqueue.worker;

/**
 * Created by kevinhe on 16/1/29.
 */

import com.jinfuzi.fund.jobqueue.jobstore.JobInfo;

/**
 * FailQueueStrategy allows for configurable failure queues.
 */
public interface FailQueueStrategy {

    /**
     * Determine the key for the failure queue.
     * A null return value is an indication that no failure queue is needed.
     * Note that the default implementation (@see DefaultFailQueueStrategy) will never return null.
     * @param thrwbl the Throwable that occurred
     * @param job the Job that failed
     * @param curQueue the queue the Job came from
     * @return the key of the failure queue to put the job into or null
     */
    String getFailQueueKey(Throwable thrwbl, JobInfo jobInfo, String curQueue);
}

