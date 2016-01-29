package com.jinfuzi.fund.jobqueue.worker;

/**
 * Created by kevinhe on 16/1/29.
 */

import com.fasterxml.jackson.core.JsonProcessingException;
import net.greghaines.jesque.worker.RecoveryStrategy;
import redis.clients.jedis.exceptions.JedisConnectionException;

/**
 * DefaultExceptionHandler reconnects if there is a connection exception, proceeds if the exception was
 * JSON-related or a thread interrupt and terminates if the executor is shutdown.
 */
public class DefaultExceptionHandler implements ExceptionHandler {

    /**
     * {@inheritDoc}
     */
    @Override
    public RecoveryStrategy onException(final JobExecutor jobExecutor, final Exception exception,
                                        final String curQueue) {
        return (exception instanceof JedisConnectionException)
                ? RecoveryStrategy.RECONNECT
                : (((exception instanceof JsonProcessingException) || ((exception instanceof InterruptedException)
                && !jobExecutor.isShutdown()))
                ? RecoveryStrategy.PROCEED
                : RecoveryStrategy.TERMINATE);
    }
}

