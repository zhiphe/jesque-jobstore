package com.jinfuzi.fund.longregression;

import com.jinfuzi.fund.jobqueue.client.JobStoreClientImpl;
import com.jinfuzi.fund.jobqueue.jobstore.JobStoreFactory;
import com.jinfuzi.fund.jobqueue.jobstore.springdata.SpringDataRedisJobStoreConfig;
import com.jinfuzi.fund.jobqueue.jobstore.springdata.SpringDataRedisStoreBuilder;
import com.jinfuzi.fund.jobqueue.worker.JobStoreWorkerImpl;
import net.greghaines.jesque.Job;
import net.greghaines.jesque.client.Client;
import net.greghaines.jesque.worker.MapBasedJobFactory;
import net.greghaines.jesque.worker.Worker;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.data.redis.core.RedisTemplate;
import org.springframework.test.context.ContextConfiguration;
import org.springframework.test.context.junit4.SpringJUnit4ClassRunner;

import java.util.Arrays;

import static net.greghaines.jesque.utils.JesqueUtils.entry;
import static net.greghaines.jesque.utils.JesqueUtils.map;

/**
 * Created by kevinhe on 16/1/29.
 */
@RunWith(SpringJUnit4ClassRunner.class)
@ContextConfiguration(locations = {"/spring-data-redis-context.xml"})
public class SpringDataRedisJobStoreQueueTest {
    private static final String testQueue = "springdataredisqueue";

    @Autowired
    private RedisTemplate redisTemplate;

    @Test
    public void test_simplequeue() throws Exception {
        // Enqueue the job before worker is created and started
        SpringDataRedisJobStoreConfig springDataRedisJobStoreConfig =
                new SpringDataRedisJobStoreConfig(
                "localhost", 6379, "springdataredis", 0
        );
        SpringDataRedisStoreBuilder jobStoreBuilder = new SpringDataRedisStoreBuilder();
        jobStoreBuilder.addRedisTemplate(springDataRedisJobStoreConfig, redisTemplate);
        JobStoreFactory.addJobStoreBuilder(springDataRedisJobStoreConfig,
                jobStoreBuilder);

        final Job job = new Job("TestAction",
                new Object[]{1, 2.3, true, "test", Arrays.asList("inner", 4.5)});
        final Client client = new JobStoreClientImpl(springDataRedisJobStoreConfig);
        try {
            client.enqueue(testQueue, job);
        } finally {
            client.end();
        }
        // Create and start worker
        final Worker worker = new JobStoreWorkerImpl(springDataRedisJobStoreConfig,
                Arrays.asList(testQueue),
                new MapBasedJobFactory(map(entry("TestAction", TestAction.class))));
        final Thread workerThread = new Thread(worker);
        workerThread.start();
        try { // Wait a bit to ensure the worker had time to process the job
            Thread.sleep(500);
        } finally { // Stop the worker
            TestUtils.stopWorker(worker, workerThread);
        }
    }
}
