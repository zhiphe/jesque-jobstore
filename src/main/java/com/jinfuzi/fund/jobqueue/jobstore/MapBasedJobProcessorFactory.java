package com.jinfuzi.fund.jobqueue.jobstore;

import net.greghaines.jesque.utils.ReflectionUtils;

import java.util.HashMap;
import java.util.Map;

/**
 * Created by kevinhe on 16/1/29.
 */
public class MapBasedJobProcessorFactory implements JobProcessorFactory {
    private Map<String, Class<? extends JobProcessor>> jobProcessorMap = new HashMap<>();

    public void addJobProcessor(String type, Class<? extends JobProcessor> jobProcessorClass) {
        jobProcessorMap.put(type, jobProcessorClass);
    }

    public MapBasedJobProcessorFactory(Map<String, Class<? extends JobProcessor>> jobProcessorMap) {
        this.jobProcessorMap = jobProcessorMap;
    }

    @Override
    public JobProcessor getJobProcessor(String type, String jobJson) throws Exception {
        return ReflectionUtils.createObject(jobProcessorMap.get(type), jobJson);

    }
}
