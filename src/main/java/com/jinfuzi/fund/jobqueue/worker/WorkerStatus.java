package com.jinfuzi.fund.jobqueue.worker;

/**
 * Created by kevinhe on 16/1/29.
 */

import com.fasterxml.jackson.annotation.JsonProperty;
import com.jinfuzi.fund.jobqueue.jobstore.JobInfo;
import net.greghaines.jesque.utils.JesqueUtils;

import java.io.Serializable;
import java.util.Date;

/**
 * A bean to hold information about the status of a Worker.
 *
 * @author Greg Haines
 */
public class WorkerStatus implements Serializable {

    private static final long serialVersionUID = 1852915628988733048L;

    @JsonProperty("run_at")
    private Date runAt;
    @JsonProperty
    private String queue;
    @JsonProperty
    private JobInfo jobInfo;
    @JsonProperty
    private boolean paused = false;

    /**
     * No-argument constructor.
     */
    public WorkerStatus() {
        // Do nothing
    }

    /**
     * Cloning constructor.
     *
     * @param origStatus
     *            the status to start from
     * @throws IllegalArgumentException
     *             if the origStatus is null
     */
    public WorkerStatus(final WorkerStatus origStatus) {
        if (origStatus == null) {
            throw new IllegalArgumentException("origStatus must not be null");
        }
        this.runAt = origStatus.runAt;
        this.queue = origStatus.queue;
        this.jobInfo = origStatus.jobInfo;
        this.paused = origStatus.paused;
    }

    /**
     * @return when the Worker started on the current job
     */
    public Date getRunAt() {
        return this.runAt;
    }

    /**
     * Set when the Worker started on the current job.
     *
     * @param runAt
     *            when the Worker started on the current job
     */
    public void setRunAt(final Date runAt) {
        this.runAt = runAt;
    }

    /**
     * @return which queue the current job came from
     */
    public String getQueue() {
        return this.queue;
    }

    /**
     * Set which queue the current job came from.
     *
     * @param queue
     *            which queue the current job came from
     */
    public void setQueue(final String queue) {
        this.queue = queue;
    }

    /**
     * @return the job
     */
    public JobInfo getJobInfo() {
        return this.jobInfo;
    }

    /**
     * Set the job.
     *
     * @param jobInfo
     *            the job
     */
    public void setJobInfo(final JobInfo jobInfo) {
        this.jobInfo = jobInfo;
    }

    /**
     * @return true if the worker is paused
     */
    public boolean isPaused() {
        return this.paused;
    }

    /**
     * Sets whether the worker is paused.
     *
     * @param paused
     *            whether the worker is paused
     */
    public void setPaused(final boolean paused) {
        this.paused = paused;
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public String toString() {
        return "WorkerStatus [queue=" + this.queue + ", runAt=" + this.runAt
                + ", paused=" + Boolean.toString(this.paused) + ", jobInfo=" + this.jobInfo + "]";
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public int hashCode() {
        final int prime = 31;
        int result = 1;
        result = prime * result + (this.paused ? 1231 : 1237);
        result = prime * result + ((this.jobInfo == null) ? 0 : this.jobInfo.hashCode());
        result = prime * result + ((this.queue == null) ? 0 : this.queue.hashCode());
        result = prime * result + ((this.runAt == null) ? 0 : this.runAt.hashCode());
        return result;
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public boolean equals(final Object obj) {
        boolean equal = false;
        if (this == obj) {
            equal = true;
        } else if (obj instanceof WorkerStatus) {
            final WorkerStatus other = (WorkerStatus) obj;
            equal = ((this.paused == other.paused)
                    && JesqueUtils.nullSafeEquals(this.queue, other.queue)
                    && JesqueUtils.nullSafeEquals(this.runAt, other.runAt)
                    && JesqueUtils.nullSafeEquals(this.jobInfo, other.jobInfo));
        }
        return equal;
    }
}

