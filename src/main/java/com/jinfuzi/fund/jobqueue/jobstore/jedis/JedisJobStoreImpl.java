package com.jinfuzi.fund.jobqueue.jobstore.jedis;

import com.jinfuzi.fund.jobqueue.jobstore.JobStore;
import net.greghaines.jesque.utils.JedisUtils;
import net.greghaines.jesque.utils.ScriptUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import redis.clients.jedis.Jedis;
import redis.clients.jedis.Transaction;
import redis.clients.jedis.exceptions.JedisConnectionException;

import java.util.Set;
import java.util.concurrent.atomic.AtomicReference;

/**
 * Created by kevinhe on 16/1/28.
 */
public class JedisJobStoreImpl implements JobStore {
    private Logger logger = LoggerFactory.getLogger(JedisJobStoreImpl.class);
    public static final String PONG = "PONG";

    private JedisJobStoreConfig jedisJobStoreConfig;
    private Jedis jedis;
    private final AtomicReference<String> popScriptHash = new AtomicReference<>(null);
    private final AtomicReference<String> lpoplpushScriptHash = new AtomicReference<>(null);

    public JedisJobStoreImpl(JedisJobStoreConfig jedisJobStoreConfig) {
        this.jedisJobStoreConfig = jedisJobStoreConfig;
        this.jedis = new Jedis(jedisJobStoreConfig.getHost(), jedisJobStoreConfig.getPort());
    }

    @Override
    public String getNameSpace() {
        return jedisJobStoreConfig.getNamespace();
    }

    @Override
    public void initialize() throws Exception {
        try {
            this.popScriptHash.set(this.jedis.scriptLoad(ScriptUtils.readScript("/workerScripts/jesque_pop.lua")));
            this.lpoplpushScriptHash.set(this.jedis.scriptLoad(
                    ScriptUtils.readScript("/workerScripts/jesque_lpoplpush.lua")));
        } catch (Exception ex) {
            logger.error("Uncaught exception in worker run-loop!", ex);
            throw ex;
        }
    }

    @Override
    public boolean ensureConnection() {
        final boolean jedisOK = testJedisConnection(jedis);
        if (!jedisOK) {
            try {
                this.jedis.quit();
            } catch (Exception e) {
            } // Ignore
            try {
                this.jedis.disconnect();
            } catch (Exception e) {
            } // Ignore
            this.jedis.connect();
        }
        return jedisOK;
    }

    public static boolean testJedisConnection(final Jedis jedis) {
        boolean jedisOK = false;
        try {
            jedisOK = (jedis.isConnected() && PONG.equals(jedis.ping()));
        } catch (Exception e) {
            jedisOK = false;
        }
        return jedisOK;
    }

    @Override
    public String disconnect() {
        return this.jedis.quit();
    }

    @Override
    public boolean reconnect(int reconAttempts, long reconnectSleepTime) {
        int i = 1;
        do {
            try {
                this.jedis.disconnect();
                this.jedis.getClient().resetPipelinedCount();
                try {
                    Thread.sleep(reconnectSleepTime);
                } catch (Exception e2) {
                }
                this.jedis.connect();
            } catch (JedisConnectionException jce) {
            } // Ignore bad connection attempts
            catch (Exception e3) {
                logger.error("Unknown Exception while trying to reconnect to Redis", e3);
            }
        } while (++i <= reconAttempts && !testJedisConnection(this.jedis));
        return testJedisConnection(this.jedis);
    }

    @Override
    public String authenticate() {
        if (jedisJobStoreConfig.getPassword() != null) {
            return this.jedis.auth(jedisJobStoreConfig.getPassword());
        }
        return null;
    }

    @Override
    public String select() {
        return this.jedis.select(jedisJobStoreConfig.getDatabase());
    }

    @Override
    public boolean canUseAsDelayedQueue(String key) {
        return JedisUtils.canUseAsDelayedQueue(this.jedis, key);
    }

    @Override
    public Long rightPush(String key, String... strings) {
        return this.jedis.rpush(key, strings);
    }

    @Override
    public String leftPop(String key) {
        return this.jedis.lpop(key);
    }

    @Override
    public Long addToSet(String key, String... members) {
        return this.jedis.sadd(key, members);
    }

    @Override
    public Long addToSortedSet(String key, double score, String member) {
        return this.jedis.zadd(key, score, member);
    }

    @Override
    public Long removeFromSet(String key, String... members) {
        return this.jedis.srem(key, members);
    }

    @Override
    public Long removeFromSortedSet(String key, String value) {
        return this.jedis.zrem(key, value);
    }

    @Override
    public Long expire(String key, int seconds) {
        return this.jedis.expire(key, seconds);
    }

    @Override
    public Long delete(String key) {
        return this.jedis.del(key);
    }

    @Override
    public Long delete(String... keys) {
        return this.jedis.del(keys);
    }

    @Override
    public Long setIfNotExist(String key, String value) {
        return this.jedis.setnx(key, value);
    }

    @Override
    public Long leftTime(String key) {
        return this.jedis.ttl(key);
    }

    @Override
    public Boolean contains(String key) {
        return this.jedis.exists(key);
    }

    @Override
    public String get(String key) {
        return this.jedis.get(key);
    }

    @Override
    public String set(String key, String value) {
        return this.jedis.set(key, value);
    }

    @Override
    public Long increase(String key) {
        return this.jedis.incr(key);
    }

    @Override
    public Set<String> memberOfSet(String key) {
        return this.jedis.smembers(key);
    }

    @Override
    public String leftPopAndPush(String popKey, String pushKey) {
        return (String) this.jedis.evalsha(this.lpoplpushScriptHash.get(), 2, popKey, pushKey);
    }

    @Override
    public String pop(String queueKey, String inFlightKey, String freqKey, String now) {
        return (String) this.jedis.evalsha(this.popScriptHash.get(), 3, queueKey, inFlightKey, freqKey, now);

    }

    @Override
    public boolean canUseAsRecurringQueue(String queueKey, String hashKey) {
        return JedisUtils.canUseAsRecurringQueue(this.jedis, queueKey, hashKey);
    }

    @Override
    public void addRecurringJob(String queueKey, long future, String hashKey, String jobJson, long frequency) {
        Transaction transaction = jedis.multi();
        transaction.zadd(queueKey, future, jobJson);
        transaction.hset(hashKey, jobJson, String.valueOf(frequency));
        if (transaction.exec() == null) {
            throw new RuntimeException("cannot add " + jobJson + " to recurring queue");
        }
    }

    @Override
    public void removeRecurringJob(String queueKey, String hashKey, String jobJson) {
        Transaction transaction = this.jedis.multi();
        transaction.hdel(hashKey, jobJson);
        transaction.zrem(queueKey, jobJson);
        if (transaction.exec() == null) {
            throw new RuntimeException("cannot remove " + jobJson + " from recurring queue");
        }
    }
}
