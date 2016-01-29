package com.jinfuzi.fund.jobqueue.worker;

/**
 * Created by kevinhe on 16/1/29.
 */
import static net.greghaines.jesque.utils.ResqueConstants.FAILED;

import com.jinfuzi.fund.jobqueue.jobstore.JobInfo;
import net.greghaines.jesque.utils.JesqueUtils;

/**
 * DefaultFailQueueStrategy puts all jobs in the standard Redis failure queue.
 */
public class DefaultFailQueueStrategy implements FailQueueStrategy {

    private final String namespace;

    /**
     * Constructor.
     * @param namespace the Redis namespace.
     */
    public DefaultFailQueueStrategy(final String namespace) {
        this.namespace = namespace;
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public String getFailQueueKey(final Throwable thrwbl, final JobInfo jobInfo, final String curQueue) {
        return JesqueUtils.createKey(this.namespace, FAILED);
    }
}
