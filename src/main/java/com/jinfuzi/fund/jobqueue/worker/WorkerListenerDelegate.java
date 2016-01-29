package com.jinfuzi.fund.jobqueue.worker;

/**
 * Created by kevinhe on 16/1/29.
 */

import com.jinfuzi.fund.jobqueue.jobstore.JobInfo;
import net.greghaines.jesque.utils.ConcurrentHashSet;
import net.greghaines.jesque.utils.ConcurrentSet;
import net.greghaines.jesque.worker.WorkerEvent;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Collections;
import java.util.EnumMap;
import java.util.Map;

/**
 * WorkerListenerDelegate keeps track of WorkerListeners and notifies each listener when fireEvent() is invoked.
 */
public class WorkerListenerDelegate implements WorkerEventEmitter {

    private static final Logger log = LoggerFactory.getLogger(WorkerListenerDelegate.class);

    private final Map<WorkerEvent, ConcurrentSet<WorkerListener>> eventListenerMap;

    /**
     * Constructor.
     */
    public WorkerListenerDelegate() {
        final Map<WorkerEvent, ConcurrentSet<WorkerListener>> elp =
                new EnumMap<WorkerEvent, ConcurrentSet<WorkerListener>>(WorkerEvent.class);
        for (final WorkerEvent event : WorkerEvent.values()) {
            elp.put(event, new ConcurrentHashSet<WorkerListener>());
        }
        this.eventListenerMap = Collections.unmodifiableMap(elp);
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public void addListener(final WorkerListener listener) {
        addListener(listener, WorkerEvent.values());
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public void addListener(final WorkerListener listener, final WorkerEvent... events) {
        if (listener != null) {
            for (final WorkerEvent event : events) {
                final ConcurrentSet<WorkerListener> listeners = this.eventListenerMap.get(event);
                if (listeners != null) {
                    listeners.add(listener);
                }
            }
        }
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public void removeListener(final WorkerListener listener) {
        removeListener(listener, WorkerEvent.values());
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public void removeListener(final WorkerListener listener, final WorkerEvent... events) {
        if (listener != null) {
            for (final WorkerEvent event : events) {
                final ConcurrentSet<WorkerListener> listeners = this.eventListenerMap.get(event);
                if (listeners != null) {
                    listeners.remove(listener);
                }
            }
        }
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public void removeAllListeners() {
        removeAllListeners(WorkerEvent.values());
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public void removeAllListeners(final WorkerEvent... events) {
        for (final WorkerEvent event : events) {
            final ConcurrentSet<WorkerListener> listeners = this.eventListenerMap.get(event);
            if (listeners != null) {
                listeners.clear();
            }
        }
    }

    /**
     * Notify all WorkerListeners currently registered for the given WorkerEvent.
     * @param event the WorkerEvent that occurred
     * @param worker the Worker that the event occurred in
     * @param queue the queue the Worker is processing
     * @param jobInfo the Job related to the event (only supply for JOB_PROCESS, JOB_EXECUTE, JOB_SUCCESS, and
     * JOB_FAILURE events)
     * @param runner the materialized object that the Job specified (only supply for JOB_EXECUTE and
     * JOB_SUCCESS events)
     * @param result the result of the successful execution of the Job (only set for JOB_SUCCESS and if the Job was
     * a Callable that returned a value)
     * @param t the Throwable that caused the event (only supply for JOB_FAILURE and ERROR events)
     */
    public void fireEvent(final WorkerEvent event, final Worker worker, final String queue, final JobInfo jobInfo,
                          final Object runner, final Object result, final Throwable t) {
        final ConcurrentSet<WorkerListener> listeners = this.eventListenerMap.get(event);
        if (listeners != null) {
            for (final WorkerListener listener : listeners) {
                if (listener != null) {
                    try {
                        listener.onEvent(event, worker, queue, jobInfo, runner, result, t);
                    } catch (Exception e) {
                        log.error("Failure executing listener " + listener + " for event " + event
                                + " from queue " + queue + " on worker " + worker, e);
                    }
                }
            }
        }
    }
}

