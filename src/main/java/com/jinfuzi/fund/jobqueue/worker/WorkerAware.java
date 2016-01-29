package com.jinfuzi.fund.jobqueue.worker;

/**
 * Created by kevinhe on 16/1/29.
 */
/**
 * WorkerAware indicates that a materialized Job should have the Worker executing the job injected before execution.
 */
public interface WorkerAware {

    /**
     * @param worker the Worker executing the job
     */
    void setWorker(Worker worker);
}
