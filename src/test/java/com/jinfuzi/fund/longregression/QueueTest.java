package com.jinfuzi.fund.longregression;

import net.greghaines.jesque.Config;
import net.greghaines.jesque.ConfigBuilder;
import net.greghaines.jesque.Job;
import net.greghaines.jesque.worker.*;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import redis.clients.jedis.Jedis;

import java.util.Arrays;

import static com.jinfuzi.fund.longregression.TestUtils.createJedis;
import static net.greghaines.jesque.utils.JesqueUtils.createKey;
import static net.greghaines.jesque.utils.JesqueUtils.entry;
import static net.greghaines.jesque.utils.JesqueUtils.map;
import static net.greghaines.jesque.utils.ResqueConstants.*;
import static net.greghaines.jesque.utils.ResqueConstants.QUEUE;

/**
 * Created by kevinhe on 16/1/28.
 */
public class QueueTest {
    private static final Config config = new ConfigBuilder().build();
    private static final String testQueue = "foo";

    @Before
    public void resetRedis() {
        TestUtils.resetRedis(config);
    }

    @Test
    public void issue18() throws Exception {
        // Enqueue the job before worker is created and started
        final Job job = new Job("TestAction", new Object[] { 1, 2.3, true, "test", Arrays.asList("inner", 4.5) });
        TestUtils.enqueueJobs(testQueue, Arrays.asList(job), config);
        Jedis jedis = createJedis(config);
        try { // Assert that we enqueued the job
            Assert.assertEquals(Long.valueOf(1), jedis.llen(createKey(config.getNamespace(), QUEUE, testQueue)));
        } finally {
            jedis.quit();
        }

        // Create and start worker
        final Worker worker = new WorkerImpl(config, Arrays.asList(testQueue),
                new MapBasedJobFactory(map(entry("TestAction", TestAction.class))));
        final Thread workerThread = new Thread(worker);
        workerThread.start();
        try { // Wait a bit to ensure the worker had time to process the job
            Thread.sleep(500);
        } finally { // Stop the worker
            TestUtils.stopWorker(worker, workerThread);
        }

        // Assert that the job was run by the worker
        jedis = createJedis(config);
        try {
            Assert.assertEquals("1", jedis.get(createKey(config.getNamespace(), STAT, PROCESSED)));
            Assert.assertNull(jedis.get(createKey(config.getNamespace(), STAT, FAILED)));
            Assert.assertEquals(Long.valueOf(0), jedis.llen(createKey(config.getNamespace(), QUEUE, testQueue)));
        } finally {
            jedis.quit();
        }
    }

    @Test
    public void test_workerpool() throws Exception {
        // Enqueue the job before worker is created and started
        final Job job = new Job("TestAction", new Object[] { 1, 2.3, true, "test", Arrays.asList("inner", 4.5) });
        TestUtils.enqueueJobs(testQueue, Arrays.asList(job), config);
        TestUtils.enqueueJobs(testQueue, Arrays.asList(job), config);
        TestUtils.enqueueJobs(testQueue, Arrays.asList(job), config);
        TestUtils.enqueueJobs(testQueue, Arrays.asList(job), config);

        final WorkerPool workerPool = new WorkerPool(
                new WorkerImplFactory(
                        config,
                        Arrays.asList(testQueue),
                        new MapBasedJobFactory(map(entry("TestAction", TestAction.class)))),
                3
        );
        final Thread workerThread = new Thread(workerPool);
        workerThread.start();
        try { // Wait a bit to ensure the worker had time to process the job
            Thread.sleep(500);
        } finally { // Stop the worker
            TestUtils.stopWorker(workerPool, workerThread);
        }
    }

    @Test
    public void test_recurringjob() throws Exception {
        // Enqueue the job before worker is created and started
        final Job job = new Job("TestAction", new Object[] { 1, 2.3, true, "test", Arrays.asList("inner", 4.5) });
        TestUtils.recurringEnqueueJobs(testQueue, Arrays.asList(job), config);
        Jedis jedis = createJedis(config);

        // Create and start worker
        final Worker worker = new WorkerImpl(config, Arrays.asList(testQueue),
                new MapBasedJobFactory(map(entry("TestAction", TestAction.class))));
        final Thread workerThread = new Thread(worker);
        workerThread.start();
        try { // Wait a bit to ensure the worker had time to process the job
            Thread.sleep(20000);
        } finally { // Stop the worker
            TestUtils.stopWorker(worker, workerThread);
        }
    }
}
