package com.jinfuzi.fund.longregression;

import com.jinfuzi.fund.jobqueue.client.Client;
import com.jinfuzi.fund.jobqueue.client.JobStoreClientImpl;
import com.jinfuzi.fund.jobqueue.jobstore.Job;
import com.jinfuzi.fund.jobqueue.jobstore.MapBasedJobProcessorFactory;
import com.jinfuzi.fund.jobqueue.jobstore.jedis.JedisJobStoreConfig;
import com.jinfuzi.fund.jobqueue.worker.JobStoreWorkerImpl;
import com.jinfuzi.fund.jobqueue.worker.Worker;
import net.greghaines.jesque.Config;
import net.greghaines.jesque.ConfigBuilder;
import org.junit.Before;
import org.junit.Test;

import java.util.Arrays;

import static net.greghaines.jesque.utils.JesqueUtils.entry;
import static net.greghaines.jesque.utils.JesqueUtils.map;

/**
 * Created by kevinhe on 16/1/28.
 */
public class JedisJobStoreQueueTest {
    private static final Config config = new ConfigBuilder().build();
    private static final String testQueue = "foo";

    @Before
    public void resetRedis() {
        TestUtils.resetRedis(config);
    }

    @Test
    public void test_simplequeue() throws Exception {
        // Enqueue the job before worker is created and started
        final Job job = new TestJob("Hello World");
        final Client client = new JobStoreClientImpl(new JedisJobStoreConfig(
                "localhost", 6379, "simplequeue-jedisjobstore", null, 0
        ));
        try {
            client.enqueue(testQueue, job);
        } finally {
            client.end();
        }
        // Create and start worker
        final Worker worker = new JobStoreWorkerImpl(new JedisJobStoreConfig(
                "localhost", 6379, "simplequeue-jedisjobstore", null, 0
        ), Arrays.asList(testQueue),
                new MapBasedJobProcessorFactory(map(entry(TestJob.class.getName(), TestJobProcessor.class))));
        final Thread workerThread = new Thread(worker);
        workerThread.start();
        try { // Wait a bit to ensure the worker had time to process the job
            Thread.sleep(500);
        } finally { // Stop the worker
            TestUtils.stopWorker(worker, workerThread);
        }
    }

    @Test
    public void test_simplequeue_multiworkers() throws Exception {
        // Enqueue the job before worker is created and started
        final Client client = new JobStoreClientImpl(new JedisJobStoreConfig(
                "localhost", 6379, "simplequeue-jedisjobstore-multiworkers", null, 0
        ));
        // Create and start worker
        final Worker worker = new JobStoreWorkerImpl(new JedisJobStoreConfig(
                "localhost", 6379, "simplequeue-jedisjobstore-multiworkers", null, 0
        ), Arrays.asList(testQueue),
                new MapBasedJobProcessorFactory(map(entry(TestJob.class.getName(), TestJobProcessor.class))));
        final Thread workerThread = new Thread(worker);
        workerThread.start();
        final Worker worker1 = new JobStoreWorkerImpl(new JedisJobStoreConfig(
                "localhost", 6379, "simplequeue-jedisjobstore-multiworkers", null, 0
        ), Arrays.asList(testQueue),
                new MapBasedJobProcessorFactory(map(entry(TestJob.class.getName(), TestJobProcessor.class))));
        final Thread workerThread1 = new Thread(worker1);
        workerThread1.start();

        try {
            client.enqueue(testQueue, new TestJob("Hello World"));
            client.enqueue(testQueue, new TestJob("Hello Kevin"));
            client.enqueue(testQueue, new TestJob("Hello Turkey"));
        } finally {
            client.end();
        }

        try { // Wait a bit to ensure the worker had time to process the job
            Thread.sleep(500);
        } finally { // Stop the worker
            TestUtils.stopWorker(worker, workerThread);
        }
    }

    @Test
    public void test_delayqueue() throws Exception {
        // Enqueue the job before worker is created and started
        final Client client = new JobStoreClientImpl(new JedisJobStoreConfig(
                "localhost", 6379, "delayqueue-jedisjobstore", null, 0
        ));
        final long future = System.currentTimeMillis() + 10 * 1000;
        try {
            client.delayedEnqueue(testQueue, new TestJob("Hello World"), future);
        } finally {
            client.end();
        }
        // Create and start worker
        final Worker worker = new JobStoreWorkerImpl(new JedisJobStoreConfig(
                "localhost", 6379, "delayqueue-jedisjobstore", null, 0
        ), Arrays.asList(testQueue),
                new MapBasedJobProcessorFactory(map(entry(TestJob.class.getName(), TestJobProcessor.class))));
        final Thread workerThread = new Thread(worker);
        workerThread.start();
        try { // Wait a bit to ensure the worker had time to process the job
            Thread.sleep(20000);
        } finally { // Stop the worker
            TestUtils.stopWorker(worker, workerThread);
        }
    }

    @Test
    public void test_recurringqueue() throws Exception {
        // Enqueue the job before worker is created and started
        final Client client = new JobStoreClientImpl(new JedisJobStoreConfig(
                "localhost", 6379, "recurringqueue-jedisjobstore", null, 0
        ));
        final long future = System.currentTimeMillis() + 1 * 1000;
        final long frequency = 1000;
        try {
            client.recurringEnqueue(testQueue, new TestJob("Hello World"), future, frequency);
        } finally {
            client.end();
        }
        // Create and start worker
        final Worker worker = new JobStoreWorkerImpl(new JedisJobStoreConfig(
                "localhost", 6379, "recurringqueue-jedisjobstore", null, 0
        ), Arrays.asList(testQueue),
                new MapBasedJobProcessorFactory(map(entry(TestJob.class.getName(), TestJobProcessor.class))));
        final Thread workerThread = new Thread(worker);
        workerThread.start();
        try { // Wait a bit to ensure the worker had time to process the job
            Thread.sleep(10000);
        } finally { // Stop the worker
            TestUtils.stopWorker(worker, workerThread);
        }
    }
}
