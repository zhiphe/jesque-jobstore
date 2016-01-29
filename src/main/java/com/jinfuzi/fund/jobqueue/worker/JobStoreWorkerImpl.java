package com.jinfuzi.fund.jobqueue.worker;

import com.fasterxml.jackson.core.JsonParseException;
import com.fasterxml.jackson.databind.JsonMappingException;
import com.jinfuzi.fund.jobqueue.jobstore.*;
import net.greghaines.jesque.json.ObjectMapperFactory;
import net.greghaines.jesque.utils.JesqueUtils;
import net.greghaines.jesque.utils.VersionUtils;
import net.greghaines.jesque.worker.RecoveryStrategy;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import redis.clients.jedis.exceptions.JedisException;

import java.io.IOException;
import java.lang.management.ManagementFactory;
import java.net.InetAddress;
import java.net.UnknownHostException;
import java.text.SimpleDateFormat;
import java.util.Collection;
import java.util.Collections;
import java.util.Date;
import java.util.concurrent.BlockingDeque;
import java.util.concurrent.LinkedBlockingDeque;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.AtomicReference;

import static net.greghaines.jesque.utils.ResqueConstants.*;
import static com.jinfuzi.fund.jobqueue.worker.JobExecutor.State.*;
import static net.greghaines.jesque.worker.WorkerEvent.*;

/**
 * Created by kevinhe on 16/1/27.
 */
public class JobStoreWorkerImpl implements Worker {
    private Logger logger = LoggerFactory.getLogger(JobStoreWorkerImpl.class);
    private static final AtomicLong WORKER_COUNTER = new AtomicLong(0);
    protected static final long EMPTY_QUEUE_SLEEP_TIME = 500; // 500 ms
    protected static final long RECONNECT_SLEEP_TIME = 5000; // 5 sec
    protected static final int RECONNECT_ATTEMPTS = 120; // Total time: 10 min

    private static volatile boolean threadNameChangingEnabled = false;

    private JobStore jobStore;
    private final String name;
    private final long workerId = WORKER_COUNTER.getAndIncrement();
    protected final WorkerListenerDelegate listenerDelegate = new WorkerListenerDelegate();
    protected final AtomicReference<State> state = new AtomicReference<State>(NEW);
    private final AtomicBoolean paused = new AtomicBoolean(false);
    private final AtomicBoolean processingJob = new AtomicBoolean(false);
    protected final BlockingDeque<String> queueNames = new LinkedBlockingDeque<String>();
    private final AtomicReference<Thread> threadRef = new AtomicReference<Thread>(null);
    private final AtomicReference<ExceptionHandler> exceptionHandlerRef =
            new AtomicReference<ExceptionHandler>(new DefaultExceptionHandler());
    private final AtomicReference<FailQueueStrategy> failQueueStrategyRef;
    private final JobProcessorFactory jobProcessorFactory;

    private final String threadNameBase = "Worker-" + this.workerId + " Jesque-" + VersionUtils.getVersion() + ": ";


    public JobStoreWorkerImpl(final JobStoreConfig jobStoreConfig, final Collection<String> queues, final JobProcessorFactory jobProcessorFactory) {
        this(jobStoreConfig, queues, jobProcessorFactory, JobStoreFactory.buildJobStore(jobStoreConfig));
    }

    public JobStoreWorkerImpl(final JobStoreConfig jobStoreConfig, final Collection<String> queues, final JobProcessorFactory jobProcessorFactory,
                              final JobStore jobStore) {
        if (jobStoreConfig == null) {
            throw new IllegalArgumentException("config must not be null");
        }
        if (jobProcessorFactory == null) {
            throw new IllegalArgumentException("jobProcessorFactory must not be null");
        }
        if (jobStore == null) {
            throw new IllegalArgumentException("jobStore must not be null");
        }
        checkQueues(queues);
        this.jobProcessorFactory = jobProcessorFactory;
        this.jobStore = jobStore;
        this.failQueueStrategyRef = new AtomicReference<FailQueueStrategy>(
                new DefaultFailQueueStrategy(jobStore.getNameSpace()));
        authenticateAndSelectDB();
        setQueues(queues);
        this.name = createName();
    }

    protected String createName() {
        final StringBuilder buf = new StringBuilder(128);
        try {
            buf.append(InetAddress.getLocalHost().getHostName()).append(COLON)
                    .append(ManagementFactory.getRuntimeMXBean().getName().split("@")[0]) // PID
                    .append('-').append(this.workerId).append(COLON).append(JAVA_DYNAMIC_QUEUES);
            for (final String queueName : this.queueNames) {
                buf.append(',').append(queueName);
            }
        } catch (UnknownHostException uhe) {
            throw new RuntimeException(uhe);
        }
        return buf.toString();
    }

    public String getName() {
        return this.name;
    }

    public boolean isPaused() {
        return this.paused.get();
    }

    public void togglePause(boolean paused) {
        this.paused.set(paused);
        synchronized (this.paused) {
            this.paused.notifyAll();
        }
    }

    public Collection<String> getQueues() {
        return Collections.unmodifiableCollection(this.queueNames);
    }

    public void addQueue(String queueName) {
        if (queueName == null || "".equals(queueName)) {
            throw new IllegalArgumentException("queueName must not be null or empty: " + queueName);
        }
        this.queueNames.add(queueName);
    }

    public void removeQueue(final String queueName, final boolean all) {
        if (queueName == null || "".equals(queueName)) {
            throw new IllegalArgumentException("queueName must not be null or empty: " + queueName);
        }
        if (all) { // Remove all instances
            boolean tryAgain = true;
            while (tryAgain) {
                tryAgain = this.queueNames.remove(queueName);
            }
        } else { // Only remove one instance
            this.queueNames.remove(queueName);
        }
    }

    public void removeAllQueues() {
        this.queueNames.clear();
    }

    public void setQueues(final Collection<String> queues) {
        checkQueues(queues);
        this.queueNames.clear();
        this.queueNames.addAll((queues == ALL_QUEUES) // Using object equality on purpose
                ? this.jobStore.memberOfSet(key(QUEUES)) // Like '*' in other clients
                : queues);
    }

    protected String key(final String... parts) {
        return JesqueUtils.createKey(this.jobStore.getNameSpace(), parts);
    }

    protected void checkQueues(final Iterable<String> queues) {
        if (queues == null) {
            throw new IllegalArgumentException("queues must not be null");
        }
        for (final String queue : queues) {
            if (queue == null || "".equals(queue)) {
                throw new IllegalArgumentException("queues' members must not be null: " + queues);
            }
        }
    }

    public WorkerEventEmitter getWorkerEventEmitter() {
        return this.listenerDelegate;
    }

    public JobProcessorFactory getJobProcessorFactory() {
        return this.jobProcessorFactory;
    }

    public ExceptionHandler getExceptionHandler() {
        return this.exceptionHandlerRef.get();
    }

    public void setExceptionHandler(ExceptionHandler exceptionHandler) {
        if (exceptionHandler == null) {
            throw new IllegalArgumentException("exceptionHandler must not be null");
        }
        this.exceptionHandlerRef.set(exceptionHandler);
    }

    public void end(boolean now) {
        if (now) {
            this.state.set(SHUTDOWN_IMMEDIATE);
            final Thread workerThread = this.threadRef.get();
            if (workerThread != null) {
                workerThread.interrupt();
            }
        } else {
            this.state.set(SHUTDOWN);
        }
        togglePause(false); // Release any threads waiting in checkPaused()

    }

    public boolean isShutdown() {
        return SHUTDOWN.equals(this.state.get()) || SHUTDOWN_IMMEDIATE.equals(this.state.get());
    }

    public boolean isProcessingJob() {
        return this.processingJob.get();
    }

    public void join(long millis) throws InterruptedException {
        final Thread workerThread = this.threadRef.get();
        if (workerThread != null && workerThread.isAlive()) {
            workerThread.join(millis);
        }
    }

    public void run() {
        if (this.state.compareAndSet(NEW, RUNNING)) {
            try {
                renameThread("RUNNING");
                this.threadRef.set(Thread.currentThread());
                this.jobStore.initialize();
                this.jobStore.addToSet(key(WORKERS), this.name);
                this.jobStore.set(key(WORKER, this.name, STARTED), new SimpleDateFormat(DATE_FORMAT).format(new Date()));
                this.listenerDelegate.fireEvent(WORKER_START, this, null, null, null, null, null);
                poll();
            } catch (Exception ex) {
                logger.error("Uncaught exception in worker run-loop!", ex);
                this.listenerDelegate.fireEvent(WORKER_ERROR, this, null, null, null, null, ex);
            } finally {
                renameThread("STOPPING");
                this.listenerDelegate.fireEvent(WORKER_STOP, this, null, null, null, null, null);
                this.jobStore.removeFromSet(key(WORKERS), this.name);
                this.jobStore.delete(key(WORKER, this.name), key(WORKER, this.name, STARTED), key(STAT, FAILED, this.name),
                        key(STAT, PROCESSED, this.name));
                this.jobStore.disconnect();
                this.threadRef.set(null);
            }
        } else if (RUNNING.equals(this.state.get())) {
            throw new IllegalStateException("This WorkerImpl is already running");
        } else {
            throw new IllegalStateException("This WorkerImpl is shutdown");
        }
    }

    protected void renameThread(final String msg) {
        Thread.currentThread().setName(this.threadNameBase + msg);
    }

    protected void poll() {
        int missCount = 0;
        String curQueue = null;
        while (RUNNING.equals(this.state.get())) {
            try {
                if (threadNameChangingEnabled) {
                    renameThread("Waiting for " + JesqueUtils.join(",", this.queueNames));
                }
                curQueue = this.queueNames.poll(EMPTY_QUEUE_SLEEP_TIME, TimeUnit.MILLISECONDS);
                if (curQueue != null) {
                    this.queueNames.add(curQueue); // Rotate the queues
                    checkPaused();
                    // Might have been waiting in poll()/checkPaused() for a while
                    if (RUNNING.equals(this.state.get())) {
                        this.listenerDelegate.fireEvent(WORKER_POLL, this, curQueue, null, null, null, null);
                        final String payload = pop(curQueue);
                        if (payload != null) {
                            process(JobUtils.deserializeJobInfo(payload), curQueue);
                            missCount = 0;
                        } else if (++missCount >= this.queueNames.size() && RUNNING.equals(this.state.get())) {
                            // Keeps worker from busy-spinning on empty queues
                            missCount = 0;
                            Thread.sleep(EMPTY_QUEUE_SLEEP_TIME);
                        }
                    }
                }
            } catch (InterruptedException ie) {
                if (!isShutdown()) {
                    recoverFromException(curQueue, ie);
                }
            } catch (JsonParseException | JsonMappingException e) {
                // If the job JSON is not deserializable, we never want to submit it again...
                removeInFlight(curQueue);
                recoverFromException(curQueue, e);
            } catch (Exception e) {
                recoverFromException(curQueue, e);
            }
        }
    }

    private void removeInFlight(final String curQueue) {
        if (SHUTDOWN_IMMEDIATE.equals(this.state.get())) {
            this.jobStore.leftPopAndPush(key(INFLIGHT, this.name, curQueue), key(QUEUE, curQueue));
        } else {
            this.jobStore.leftPop(key(INFLIGHT, this.name, curQueue));
        }
    }

    protected void recoverFromException(final String curQueue, final Exception ex) {
        final RecoveryStrategy recoveryStrategy = this.exceptionHandlerRef.get().onException(this, ex, curQueue);
        switch (recoveryStrategy) {
            case RECONNECT:
                logger.info("Reconnecting to Redis in response to exception", ex);
                final int reconAttempts = getReconnectAttempts();
                if (!this.jobStore.reconnect(reconAttempts, RECONNECT_SLEEP_TIME)) {
                    logger.warn("Terminating in response to exception after " + reconAttempts + " to reconnect", ex);
                    end(false);
                } else {
                    authenticateAndSelectDB();
                    logger.info("Reconnected to Redis");
                }
                break;
            case TERMINATE:
                logger.warn("Terminating in response to exception", ex);
                end(false);
                break;
            case PROCEED:
                this.listenerDelegate.fireEvent(WORKER_ERROR, this, curQueue, null, null, null, ex);
                break;
            default:
                logger.error("Unknown RecoveryStrategy: " + recoveryStrategy
                        + " while attempting to recover from the following exception; worker proceeding...", ex);
                break;
        }
    }

    private void authenticateAndSelectDB() {
        this.jobStore.authenticate();
        this.jobStore.select();
    }

    protected int getReconnectAttempts() {
        return RECONNECT_ATTEMPTS;
    }

    protected void success(final JobInfo jobInfo, final Object runner, final Object result, final String curQueue) {
        // The job may have taken a long time; make an effort to ensure the
        // connection is OK
        this.jobStore.ensureConnection();
        try {
            this.jobStore.increase(key(STAT, PROCESSED));
            this.jobStore.increase(key(STAT, PROCESSED, this.name));
        } catch (JedisException je) {
            logger.warn("Error updating success stats for job = " + jobInfo, je);
        }
        this.listenerDelegate.fireEvent(JOB_SUCCESS, this, curQueue, jobInfo, runner, result, null);
    }

    protected void process(final JobInfo jobInfo, final String curQueue) {
        try {
            this.processingJob.set(true);
            if (threadNameChangingEnabled) {
                renameThread("Processing " + curQueue + " since " + System.currentTimeMillis());
            }
            final JobProcessor jobProcessor = this.jobProcessorFactory.getJobProcessor(jobInfo.getJobType(), jobInfo.getJobJson());
            this.listenerDelegate.fireEvent(JOB_PROCESS, this, curQueue, jobInfo, null, null, null);
            this.jobStore.set(key(WORKER, this.name), statusMsg(curQueue, jobInfo));
            final JobResult result = execute(jobInfo, curQueue, jobProcessor);
            success(jobInfo, jobProcessor, result, curQueue);
        } catch (Throwable thrwbl) {
            failure(thrwbl, jobInfo, curQueue);
        } finally {
            removeInFlight(curQueue);
            this.jobStore.delete(key(WORKER, this.name));
            this.processingJob.set(false);
        }
    }

    protected void failure(final Throwable thrwbl, final JobInfo jobInfo, final String curQueue) {
        // The job may have taken a long time; make an effort to ensure the connection is OK
        this.jobStore.ensureConnection();
        try {
            this.jobStore.increase(key(STAT, FAILED));
            this.jobStore.increase(key(STAT, FAILED, this.name));
            final String failQueueKey = this.failQueueStrategyRef.get().getFailQueueKey(thrwbl, jobInfo, curQueue);
            if (failQueueKey != null) {
                this.jobStore.rightPush(failQueueKey, failMsg(thrwbl, curQueue, jobInfo));
            }
        } catch (JedisException je) {
            logger.warn("Error updating failure stats for throwable=" + thrwbl + " jobInfo =" + jobInfo, je);
        } catch (IOException ioe) {
            logger.warn("Error serializing failure payload for throwable=" + thrwbl + " jobInfo =" + jobInfo, ioe);
        }
        this.listenerDelegate.fireEvent(JOB_FAILURE, this, curQueue, jobInfo, null, null, thrwbl);
    }

    protected String failMsg(final Throwable thrwbl, final String queue, final JobInfo jobInfo) throws IOException {
        final JobFailure failure = new JobFailure();
        failure.setFailedAt(new Date());
        failure.setWorker(this.name);
        failure.setQueue(queue);
        failure.setJobInfo(jobInfo);
        failure.setThrowable(thrwbl);
        return ObjectMapperFactory.get().writeValueAsString(failure);
    }

    protected JobResult execute(final JobInfo jobInfo, final String curQueue, final JobProcessor jobProcessor) throws Exception {
        if (jobProcessor instanceof WorkerAware) {
            ((WorkerAware) jobProcessor).setWorker(this);
        }
        this.listenerDelegate.fireEvent(JOB_EXECUTE, this, curQueue, jobInfo, jobProcessor, null, null);
        if (jobProcessor instanceof Runnable) {
            ((Runnable) jobProcessor).run(); // The job is executing!
        } else { // Should never happen since we're testing the class earlier
            throw new ClassCastException("Instance must be a Runnable or a Callable: " + jobProcessor.getClass().getName()
                    + " - " + jobProcessor);
        }
        return null;
    }

    protected String pop(final String curQueue) {
        final String key = key(QUEUE, curQueue);
        return this.jobStore.pop(key, key(INFLIGHT, this.name, curQueue),
                JesqueUtils.createRecurringHashKey(key), Long.toString(System.currentTimeMillis()));
    }

    protected String statusMsg(final String queue, final JobInfo jobInfo) throws IOException {
        final WorkerStatus status = new WorkerStatus();
        status.setRunAt(new Date());
        status.setQueue(queue);
        status.setJobInfo(jobInfo);
        return ObjectMapperFactory.get().writeValueAsString(status);
    }

    protected void checkPaused() throws IOException {
        if (this.paused.get()) {
            synchronized (this.paused) {
                if (this.paused.get()) {
                    this.jobStore.set(key(WORKER, this.name), pauseMsg());
                }
                while (this.paused.get()) {
                    try {
                        this.paused.wait();
                    } catch (InterruptedException ie) {
                        logger.warn("Worker interrupted", ie);
                    }
                }
                this.jobStore.delete(key(WORKER, this.name));
            }
        }
    }

    protected String pauseMsg() throws IOException {
        final WorkerStatus status = new WorkerStatus();
        status.setRunAt(new Date());
        status.setPaused(isPaused());
        return ObjectMapperFactory.get().writeValueAsString(status);
    }
}
