package com.jinfuzi.fund.jobqueue.client;

import com.jinfuzi.fund.jobqueue.jobstore.Job;

/**
 * Created by kevinhe on 16/1/29.
 */
public interface Client {
    /**
     * Queues a job in a given queue to be run.
     *
     * @param queue
     *            the queue to add the Job to
     * @param job
     *            the job to be enqueued
     * @throws IllegalArgumentException
     *             if the queue is null or empty or if the job is null
     */
    void enqueue(String queue, Job job);

    /**
     * Queues a job with high priority in a given queue to be run.
     *
     * @param queue
     *            the queue to add the Job to
     * @param job
     *            the job to be enqueued
     * @throws IllegalArgumentException
     *             if the queue is null or empty or if the job is null
     */
    void priorityEnqueue(String queue, Job job);

    /**
     * Quits the connection to the Redis server.
     */
    void end();

    /**
     * Acquire a non-blocking distributed lock. Calling this method again renews
     * the lock.
     *
     * @param lockName
     *            the name of the lock to acquire
     * @param timeout
     *            number of seconds until the lock will expire
     * @param lockHolder
     *            a unique string identifying the caller
     * @return true, if the lock was acquired, false otherwise
     */
    boolean acquireLock(String lockName, String lockHolder, int timeout);

    /**
     * Queues a job in a given queue to be run in the future.
     *
     * @param queue
     *            the queue to add the Job to
     * @param job
     *            the job to be enqueued
     * @param future
     *            timestamp when the job will run
     * @throws IllegalArgumentException
     *             if the queue is null or empty, if the job is null or if the
     *             timestamp is not in the future
     */
    void delayedEnqueue(String queue, Job job, long future);

    /**
     * Removes a queued future job.
     *
     * @param queue
     *            the queue to remove the Job from
     * @param job
     *            the job to be removed
     * @throws IllegalArgumentException
     *             if the queue is null or empty, if the job is null
     */
    void removeDelayedEnqueue(String queue, Job job);

    /**
     * Queues a job to be in the future and recur
     *
     * @param queue
     *          the queue to add the Job too
     * @param job
     *          the job to be enqueued
     * @param future
     *          timestamp when the job will run
     * @param frequency
     *          frequency in millis how often the job will run
     * @throws IllegalArgumentException
     *          if the queue is null or empty, if the job is null
     */
    void recurringEnqueue(String queue, Job job, long future, long frequency);

    /**
     * Removes a queued recurring job.
     *
     * @param queue
     *            the queue to remove the Job from
     * @param job
     *            the job to be removed
     * @throws IllegalArgumentException
     *             if the queue is null or empty, if the job is null
     */
    void removeRecurringEnqueue(String queue, Job job);
}
