package com.jinfuzi.fund.jobqueue.worker;

import com.jinfuzi.fund.jobqueue.jobstore.JobInfo;
import net.greghaines.jesque.worker.WorkerEvent;

/**
 * Created by kevinhe on 16/1/29.
 */
public interface WorkerListener {

    /**
     * This method is called by the Worker upon the occurrence of a registered WorkerEvent.
     * @param event the WorkerEvent that occurred
     * @param worker the Worker that the event occurred in
     * @param queue the queue the Worker is processing
     * @param jobInfo the Job related to the event (only set for JOB_PROCESS, JOB_EXECUTE, JOB_SUCCESS, and
     * JOB_FAILURE events)
     * @param runner the materialized object that the Job specified (only set for JOB_EXECUTE and JOB_SUCCESS events)
     * @param result the result of the successful execution of the Job (only set for JOB_SUCCESS and if the Job was
     * a Callable that returned a value)
     * @param t the Throwable that caused the event (only set for JOB_FAILURE and ERROR events)
     */
    void onEvent(WorkerEvent event, Worker worker, String queue, JobInfo jobInfo, Object runner, Object result, Throwable t);
}
