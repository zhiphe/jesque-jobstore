package com.jinfuzi.fund.jobqueue.jobstore;

/**
 * Created by kevinhe on 16/1/29.
 */
public class AbstractJob implements Job {
    private boolean valid;

    @Override
    public boolean isValid() {
        return valid;
    }

    public void setValid(boolean valid) {
        this.valid = valid;
    }
}
