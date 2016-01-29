package com.jinfuzi.fund.jobqueue.jobstore;

import com.fasterxml.jackson.core.JsonParseException;
import com.fasterxml.jackson.core.JsonProcessingException;
import net.greghaines.jesque.json.ObjectMapperFactory;

import java.io.IOException;

/**
 * Created by kevinhe on 16/1/29.
 */
public class JobUtils {
    public static String serializeJobInfo(Job job) throws JsonProcessingException {
        JobInfo jobInfo = new JobInfo(job.getClass().getName(),
                ObjectMapperFactory.get().writeValueAsString(job));
        return ObjectMapperFactory.get().writeValueAsString(jobInfo);
    }

    public static JobInfo deserializeJobInfo(String jobInfoJson) throws IOException {
        return ObjectMapperFactory.get().readValue(jobInfoJson, JobInfo.class);
    }
}
