package com.jinfuzi.fund.jobqueue.worker;

import net.greghaines.jesque.worker.RecoveryStrategy;

/**
 * Created by kevinhe on 16/1/29.
 */
public interface ExceptionHandler {
    /**
     * Called when a worker encounters an exception.
     *
     * @param jobExecutor the worker that encountered the exception
     * @param exception   the exception
     * @param curQueue    the current queue being processed
     * @return the {@link RecoveryStrategy} to use
     */
    RecoveryStrategy onException(JobExecutor jobExecutor, Exception exception, String curQueue);
}
