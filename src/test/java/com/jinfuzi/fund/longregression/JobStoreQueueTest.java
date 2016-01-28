package com.jinfuzi.fund.longregression;

import com.jinfuzi.fund.jobqueue.client.JobStoreClientImpl;
import com.jinfuzi.fund.jobqueue.jobstore.JedisJobStoreConfig;
import com.jinfuzi.fund.jobqueue.worker.JobStoreWorkerImpl;
import net.greghaines.jesque.Config;
import net.greghaines.jesque.ConfigBuilder;
import net.greghaines.jesque.Job;
import net.greghaines.jesque.client.Client;
import net.greghaines.jesque.worker.MapBasedJobFactory;
import net.greghaines.jesque.worker.Worker;
import org.junit.Before;
import org.junit.Test;

import java.util.Arrays;

import static net.greghaines.jesque.utils.JesqueUtils.entry;
import static net.greghaines.jesque.utils.JesqueUtils.map;

/**
 * Created by kevinhe on 16/1/28.
 */
public class JobStoreQueueTest {
    private static final Config config = new ConfigBuilder().build();
    private static final String testQueue = "foo";

    @Before
    public void resetRedis() {
        TestUtils.resetRedis(config);
    }

    @Test
    public void issue18() throws Exception {
        // Enqueue the job before worker is created and started
        final Job job = new Job("TestAction", new Object[]{1, 2.3, true, "test", Arrays.asList("inner", 4.5)});
        final Client client = new JobStoreClientImpl(new JedisJobStoreConfig(config));
        try {
            client.enqueue(testQueue, job);
        } finally {
            client.end();
        }
        // Create and start worker
        final Worker worker = new JobStoreWorkerImpl(new JedisJobStoreConfig(config), Arrays.asList(testQueue),
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
