package com.jinfuzi.fund.jobqueue.jobstore.springdata.redis;

import com.jinfuzi.fund.jobqueue.jobstore.JobStore;
import com.jinfuzi.fund.jobqueue.jobstore.JobStoreBuilder;
import com.jinfuzi.fund.jobqueue.jobstore.JobStoreConfig;
import org.springframework.data.redis.core.RedisTemplate;

import java.util.HashMap;
import java.util.Map;

/**
 * Created by kevinhe on 16/1/29.
 */
public class SpringDataRedisStoreBuilder implements JobStoreBuilder {
    private Map<String, RedisTemplate> redisTemplateMap = new HashMap<>();

    @Override
    public JobStore buildJobStore(JobStoreConfig jobStoreConfig) {
        SpringDataRedisJobStoreConfig springDataRedisJobStoreConfig =
                (SpringDataRedisJobStoreConfig) jobStoreConfig;
        String templateKey = buildRedisTemplateKey(springDataRedisJobStoreConfig);
        if (!redisTemplateMap.containsKey(templateKey)) {
            throw new RuntimeException("No RedisTemplate of host \"" +
            springDataRedisJobStoreConfig.getHost() +
            "\" and port \"" +
            springDataRedisJobStoreConfig.getPort() +
            "\" exists");
        }
        return new SpringDataRedisJobStoreImpl(redisTemplateMap.get(templateKey),
                (SpringDataRedisJobStoreConfig) jobStoreConfig);
    }

    public void addRedisTemplate(SpringDataRedisJobStoreConfig springDataRedisJobStoreConfig, RedisTemplate redisTemplate) {
        redisTemplateMap.put(buildRedisTemplateKey(springDataRedisJobStoreConfig), redisTemplate);
    }

    protected String buildRedisTemplateKey(SpringDataRedisJobStoreConfig springDataRedisJobStoreConfig) {
        StringBuilder key = new StringBuilder();
        key.append(springDataRedisJobStoreConfig.getHost());
        key.append("_");
        key.append(springDataRedisJobStoreConfig.getPort());
        return key.toString();
    }
}
