package com.jinfuzi.fund.longregression;

import com.jinfuzi.fund.jobqueue.jobstore.AbstractJob;

/**
 * Created by kevinhe on 16/1/29.
 */
public class TestJob extends AbstractJob {
    private String message;

    public TestJob() {

    }

    public TestJob(String message) {
        this.message = message;
        this.setValid(true);
    }

    public String getMessage() {
        return message;
    }

    public void setMessage(String message) {
        this.message = message;
    }
}
