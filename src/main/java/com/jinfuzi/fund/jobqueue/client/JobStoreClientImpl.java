package com.jinfuzi.fund.jobqueue.client;

import com.jinfuzi.fund.jobqueue.jobstore.JobStore;
import com.jinfuzi.fund.jobqueue.jobstore.JobStoreConfig;
import com.jinfuzi.fund.jobqueue.jobstore.JobStoreFactory;
import net.greghaines.jesque.Job;
import net.greghaines.jesque.client.Client;
import net.greghaines.jesque.json.ObjectMapperFactory;
import net.greghaines.jesque.utils.JesqueUtils;

import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;

import static net.greghaines.jesque.utils.ResqueConstants.QUEUE;
import static net.greghaines.jesque.utils.ResqueConstants.QUEUES;

/**
 * Created by kevinhe on 16/1/27.
 */
public class JobStoreClientImpl implements Client {
    public static final boolean DEFAULT_CHECK_CONNECTION_BEFORE_USE = false;

    private JobStoreConfig jobStoreConfig;
    private JobStore jobStore;
    private boolean checkConnectionBeforeUse;
    private ScheduledExecutorService keepAliveService = null;

    public JobStoreClientImpl(final JobStoreConfig jobStoreConfig) {
        this(jobStoreConfig, DEFAULT_CHECK_CONNECTION_BEFORE_USE);
    }

    public JobStoreClientImpl(final JobStoreConfig jobStoreConfig, final boolean checkConnectionBeforeUse) {
        init(jobStoreConfig);
        this.jobStoreConfig = jobStoreConfig;
        this.jobStore = JobStoreFactory.buildJobStore(jobStoreConfig);
        authenticateAndSelectDB();
        this.checkConnectionBeforeUse = checkConnectionBeforeUse;
        this.keepAliveService = null;
    }

    public JobStoreClientImpl(final JobStoreConfig jobStoreConfig, final long initialDelay, final long period, final TimeUnit timeUnit) {
        init(jobStoreConfig);
        this.jobStoreConfig = jobStoreConfig;
        this.jobStore = JobStoreFactory.buildJobStore(jobStoreConfig);
        authenticateAndSelectDB();
        this.checkConnectionBeforeUse = false;
        this.keepAliveService = Executors.newSingleThreadScheduledExecutor();
        this.keepAliveService.scheduleAtFixedRate(new Runnable() {
            public void run() {
                if (!jobStore.ensureConnection()) {
                    authenticateAndSelectDB();
                }
            }
        }, initialDelay, period, timeUnit);
    }

    protected void init(final JobStoreConfig jobStoreConfig) {
        if (jobStoreConfig == null) {
            throw new IllegalArgumentException("config must not be null");
        }
    }

    public void enqueue(String s, Job job) {
        validateArguments(s, job);
        try {
            doEnqueue(s, ObjectMapperFactory.get().writeValueAsString(job));
        } catch (RuntimeException re) {
            throw re;
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }

    protected void doEnqueue(final String queue, final String jobJson) {
        ensureConnection();
        doEnqueue(this.jobStore, queue, jobJson);
    }

    public static void doEnqueue(final JobStore jobStore, final String queue, final String jobJson) {
        jobStore.addToSet(JesqueUtils.createKey(jobStore.getNameSpace(), QUEUES), queue);
        jobStore.rightPush(JesqueUtils.createKey(jobStore.getNameSpace(), QUEUE, queue), jobJson);
    }

    private void ensureConnection() {
        if (this.checkConnectionBeforeUse && !this.jobStore.ensureConnection()) {
            authenticateAndSelectDB();
        }
    }

    private void authenticateAndSelectDB() {
        this.jobStore.authenticate();
        this.jobStore.select();
    }

    private void validateArguments(final String queue, final Job job) {
        if (queue == null || "".equals(queue)) {
            throw new IllegalArgumentException("queue must not be null or empty: " + queue);
        }
        if (job == null) {
            throw new IllegalArgumentException("job must not be null");
        }
        if (!job.isValid()) {
            throw new IllegalStateException("job is not valid: " + job);
        }
    }

    public void priorityEnqueue(String s, Job job) {

    }

    public void end() {
        ensureConnection();
        if (this.keepAliveService != null) {
            this.keepAliveService.shutdownNow();
        }
        this.jobStore.disconnect();
    }

    public boolean acquireLock(String lockName, String lockHolder, int timeout) {
        if ((lockName == null) || "".equals(lockName)) {
            throw new IllegalArgumentException("lockName must not be null or empty: " + lockName);
        }
        if ((lockHolder == null) || "".equals(lockHolder)) {
            throw new IllegalArgumentException("lockHolder must not be null or empty: " + lockHolder);
        }
        if (timeout < 1) {
            throw new IllegalArgumentException("timeout must be a positive number");
        }
        try {
            return doAcquireLock(lockName, lockHolder, timeout);
        } catch (RuntimeException re) {
            throw re;
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }

    protected boolean doAcquireLock(final String lockName, final String lockHolder, final int timeout) throws Exception {
        ensureConnection();
        return doAcquireLock(this.jobStore, lockName, lockHolder, timeout);
    }

    public static boolean doAcquireLock(final JobStore jobStore, final String lockName, final String lockHolder, final int timeout) {
        final String key = JesqueUtils.createKey(jobStore.getNameSpace(), lockName);
        // If lock already exists, extend it
        String existingLockHolder = jobStore.get(key);
        if ((existingLockHolder != null) && existingLockHolder.equals(lockHolder)) {
            if (jobStore.expire(key, timeout) == 1) {
                existingLockHolder = jobStore.get(key);
                if ((existingLockHolder != null) && existingLockHolder.equals(lockHolder)) {
                    return true;
                }
            }
        }
        // Check to see if the key exists and is expired for cleanup purposes
        if (jobStore.contains(key) && (jobStore.leftTime(key) < 0)) {
            // It is expired, but it may be in the process of being created, so
            // sleep and check again
            try {
                Thread.sleep(2000);
            } catch (InterruptedException ie) {
            } // Ignore interruptions
            if (jobStore.leftTime(key) < 0) {
                existingLockHolder = jobStore.get(key);
                // If it is our lock mark the time to live
                if ((existingLockHolder != null) && existingLockHolder.equals(lockHolder)) {
                    if (jobStore.expire(key, timeout) == 1) {
                        existingLockHolder = jobStore.get(key);
                        if ((existingLockHolder != null) && existingLockHolder.equals(lockHolder)) {
                            return true;
                        }
                    }
                } else { // The key is expired, whack it!
                    jobStore.delete(key);
                }
            } else { // Someone else locked it while we were sleeping
                return false;
            }
        }
        // Ignore the cleanup steps above, start with no assumptions test
        // creating the key
        if (jobStore.setIfNotExist(key, lockHolder) == 1) {
            // Created the lock, now set the expiration
            if (jobStore.expire(key, timeout) == 1) { // Set the timeout
                existingLockHolder = jobStore.get(key);
                if ((existingLockHolder != null) && existingLockHolder.equals(lockHolder)) {
                    return true;
                }
            } else { // Don't know why it failed, but for now just report failed
                // acquisition
                return false;
            }
        }
        // Failed to create the lock
        return false;
    }

    public void delayedEnqueue(String s, Job job, long l) {

    }

    public void removeDelayedEnqueue(String s, Job job) {

    }

    public void recurringEnqueue(String s, Job job, long l, long l1) {

    }

    public void removeRecurringEnqueue(String s, Job job) {

    }
}
