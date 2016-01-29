package com.jinfuzi.fund.jobqueue.worker;

import com.jinfuzi.fund.jobqueue.jobstore.JobProcessorFactory;

/**
 * Created by kevinhe on 16/1/29.
 */
public interface JobExecutor {

    /**
     * States of the job executor.
     */
    public enum State {
        /**
         * The JobExecutor has not started running.
         */
        NEW,
        /**
         * The JobExecutor is currently running.
         */
        RUNNING,
        /**
         * The JobExecutor has shutdown.
         */
        SHUTDOWN,
        /**
         * The JobExecutor has shutdown, interrupting running jobs.
         */
        SHUTDOWN_IMMEDIATE;
    }

    /**
     * The job factory.
     * @return the job factory
     */
    JobProcessorFactory getJobProcessorFactory();

    /**
     * The current exception handler.
     * @return the current exception handler
     */
    ExceptionHandler getExceptionHandler();

    /**
     * Set this JobExecutor's exception handler to the given handler.
     * @param exceptionHandler the exception handler to use
     */
    void setExceptionHandler(ExceptionHandler exceptionHandler);

    /**
     * Shutdown this JobExecutor.
     * @param now if true, an effort will be made to stop any job in progress
     */
    void end(boolean now);

    /**
     * Returns whether this JobExecutor is either shutdown or in the process of shutting down.
     * @return true if this JobExecutor is either shutdown or in the process of shutting down
     */
    boolean isShutdown();

    /**
     * Returns whether this JobExecutor is currently processing a job.
     * @return true if this JobExecutor is currently processing a job
     */
    boolean isProcessingJob();

    /**
     * Wait for this JobExecutor to complete. A timeout of 0 means to wait forever.
     * This method will only return after a thread has called {@link #end(boolean)}.
     * @param millis the time to wait in milliseconds
     * @throws InterruptedException if any thread has interrupted the current thread
     */
    void join(long millis) throws InterruptedException;

}
