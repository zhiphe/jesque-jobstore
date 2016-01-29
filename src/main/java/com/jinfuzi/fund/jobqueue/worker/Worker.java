package com.jinfuzi.fund.jobqueue.worker;

import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;

/**
 * Created by kevinhe on 16/1/29.
 */
public interface Worker extends JobExecutor, Runnable {

    /**
     * Special value to tell a Worker to poll all currently available queues.
     */
    Collection<String> ALL_QUEUES = Collections.unmodifiableList(Arrays.asList("*"));

    /**
     * Returns the name of this Worker.
     * @return the name of this Worker
     */
    String getName();

    /**
     * Returns whether this worker is paused.
     * @return whether this worker is paused. If true, the worker is not processing any new jobs;
     * if false, the worker is processing new jobs
     */
    boolean isPaused();

    /**
     * Toggle whether this worker will process any new jobs.
     * @param paused if true, the worker will not process any new jobs; if false, the worker will process new jobs
     */
    void togglePause(boolean paused);

    /**
     * The queues that this Worker will poll.
     * @return an unmodifiable view of the queues to be polled
     */
    Collection<String> getQueues();

    /**
     * Poll the given queue. If the queue exists multiple times, it will be checked that many times per loop.
     * This allows for a queue to be given higher priority by checking it more often.
     * @param queueName the name of the queue to poll
     */
    void addQueue(String queueName);

    /**
     * Stop polling the given queue. If the <code>all</code> argument is true, all instances of the queue will be
     * removed, otherwise, only one instance is removed.
     * @param queueName the queue to stop polling
     * @param all whether to remove all or only one of the instances
     */
    void removeQueue(String queueName, boolean all);

    /**
     * Stop polling all queues.
     */
    void removeAllQueues();

    /**
     * Clear any current queues and poll the given queues.
     * @param queues the queues to poll
     */
    void setQueues(Collection<String> queues);

    /**
     * @return the worker event emitter for this worker
     */
    WorkerEventEmitter getWorkerEventEmitter();
}

