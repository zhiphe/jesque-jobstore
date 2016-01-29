package com.jinfuzi.fund.jobqueue.jobstore;

/**
 * Created by kevinhe on 16/1/29.
 */
public class JobInfo {
    private String jobType;
    private String jobJson;

    public JobInfo() {

    }

    public JobInfo(String jobType, String jobJson) {
        this.jobType = jobType;
        this.jobJson = jobJson;
    }

    public String getJobType() {
        return jobType;
    }

    public void setJobType(String jobType) {
        this.jobType = jobType;
    }

    public String getJobJson() {
        return jobJson;
    }

    public void setJobJson(String jobJson) {
        this.jobJson = jobJson;
    }
}
