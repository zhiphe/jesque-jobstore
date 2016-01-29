package com.jinfuzi.fund.jobqueue.jobstore.springdata;

import com.jinfuzi.fund.jobqueue.jobstore.JobStore;
import net.greghaines.jesque.utils.ScriptUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.dao.DataAccessException;
import org.springframework.data.redis.connection.RedisConnection;
import org.springframework.data.redis.core.RedisCallback;
import org.springframework.data.redis.core.RedisTemplate;
import org.springframework.data.redis.core.script.DefaultRedisScript;

import java.util.*;
import java.util.concurrent.atomic.AtomicReference;
import java.util.stream.Collectors;

/**
 * Created by kevinhe on 16/1/28.
 */
public class SpringDataRedisJobStoreImpl implements JobStore {
    private Logger logger = LoggerFactory.getLogger(SpringDataRedisJobStoreImpl.class);

    private final RedisTemplate redisTemplate;
    private final SpringDataRedisJobStoreConfig springDataRedisJobStoreConfig;
    private final AtomicReference<DefaultRedisScript> popScriptHash = new AtomicReference<>(null);
    private final AtomicReference<DefaultRedisScript> lpoplpushScriptHash = new AtomicReference<>(null);

    public SpringDataRedisJobStoreImpl(
            RedisTemplate redisTemplate,
            SpringDataRedisJobStoreConfig springDataRedisJobStoreConfig) {
        this.redisTemplate = redisTemplate;
        this.springDataRedisJobStoreConfig = springDataRedisJobStoreConfig;
    }

    @Override
    public String getNameSpace() {
        return springDataRedisJobStoreConfig.getNamespace();
    }

    @Override
    public void initialize() throws Exception {
        try {

            this.popScriptHash.set(new DefaultRedisScript(
                    ScriptUtils.readScript("/workerScripts/jesque_pop.lua"),
                    String.class));
            this.lpoplpushScriptHash.set(new DefaultRedisScript(
                    ScriptUtils.readScript("/workerScripts/jesque_lpoplpush.lua"),
                    String.class));
        } catch (Exception ex) {
            logger.error("Uncaught exception in worker run-loop!", ex);
            throw ex;
        }
    }

    @Override
    public boolean ensureConnection() {
        return !((Boolean) redisTemplate.execute(new RedisCallback() {
            @Override
            public Boolean doInRedis(RedisConnection redisConnection) throws DataAccessException {
                return redisConnection.isClosed();
            }
        })).booleanValue();
    }

    @Override
    public String disconnect() {
        return null;
    }

    @Override
    public boolean reconnect(int reconAttempts, long reconnectSleepTime) {
        return true;
    }

    @Override
    public String authenticate() {
        return null;
    }

    @Override
    public String select() {
        return (String) this.redisTemplate.execute(new RedisCallback() {
            @Override
            public Void doInRedis(RedisConnection redisConnection) throws DataAccessException {
                redisConnection.select(springDataRedisJobStoreConfig.getDatabase());
                return null;
            }
        });
    }

    @Override
    public Long rightPush(String key, String... strings) {
        return (Long) this.redisTemplate.execute(new RedisCallback() {
            @Override
            public Long doInRedis(RedisConnection redisConnection) throws DataAccessException {
                byte[][] stringsInByte = new byte[strings.length][];
                return redisConnection.rPush(key.getBytes(),
                        Arrays.asList(strings).
                                stream().
                                map(string -> string.getBytes()).
                                collect(Collectors.toList()).
                                toArray(stringsInByte));
            }
        });
    }

    @Override
    public String leftPop(String key) {
        return new String((byte[]) this.redisTemplate.execute(new RedisCallback() {
            @Override
            public byte[] doInRedis(RedisConnection redisConnection) throws DataAccessException {
                return redisConnection.lPop(key.getBytes());
            }
        }));
    }

    @Override
    public Long addToSet(String key, String... members) {
        return (Long) this.redisTemplate.execute(new RedisCallback() {
            @Override
            public Long doInRedis(RedisConnection redisConnection) throws DataAccessException {
                byte[][] membersInByte = new byte[members.length][];
                return redisConnection.sAdd(key.getBytes(),
                        Arrays.asList(members).
                                stream().
                                map(member -> member.getBytes()).
                                collect(Collectors.toList()).
                                toArray(membersInByte));
            }
        });
    }

    @Override
    public Long removeFromSet(String key, String... members) {
        return (Long) this.redisTemplate.execute(new RedisCallback() {
            @Override
            public Long doInRedis(RedisConnection redisConnection) throws DataAccessException {
                byte[][] membersInByte = new byte[members.length][];
                return redisConnection.sRem(key.getBytes(),
                        Arrays.asList(members).
                                stream().
                                map(member -> member.getBytes()).
                                collect(Collectors.toList()).
                                toArray(membersInByte));
            }
        });
    }

    @Override
    public Long expire(String key, int seconds) {
        return (Boolean) this.redisTemplate.execute(new RedisCallback() {
            @Override
            public Boolean doInRedis(RedisConnection redisConnection) throws DataAccessException {
                return redisConnection.expire(key.getBytes(), seconds);
            }
        }) ? 1l : 0l;
    }

    @Override
    public Long delete(String key) {
        return (Long) this.redisTemplate.execute(new RedisCallback() {
            @Override
            public Long doInRedis(RedisConnection redisConnection) throws DataAccessException {
                return redisConnection.del(key.getBytes());
            }
        });
    }

    @Override
    public Long delete(String... keys) {
        return (Long) this.redisTemplate.execute(new RedisCallback() {
            @Override
            public Long doInRedis(RedisConnection redisConnection) throws DataAccessException {
                byte[][] keysInByte = new byte[keys.length][];
                return redisConnection.del(Arrays.asList(keys).stream().map(key -> key.getBytes()).collect(Collectors.toList()).toArray(keysInByte));
            }
        });
    }

    @Override
    public Long setIfNotExist(String key, String value) {
        return (Long) this.redisTemplate.execute(new RedisCallback() {
            @Override
            public Void doInRedis(RedisConnection redisConnection) throws DataAccessException {
                redisConnection.setNX(key.getBytes(), value.getBytes());
                return null;
            }
        });
    }

    @Override
    public Long leftTime(String key) {
        return (Long) this.redisTemplate.execute(new RedisCallback() {
            @Override
            public Long doInRedis(RedisConnection redisConnection) throws DataAccessException {
                return redisConnection.ttl(key.getBytes());
            }
        });
    }

    @Override
    public Boolean contains(String key) {
        return (Boolean) this.redisTemplate.execute(new RedisCallback() {
            @Override
            public Boolean doInRedis(RedisConnection redisConnection) throws DataAccessException {
                return redisConnection.exists(key.getBytes());
            }
        });
    }

    @Override
    public String get(String key) {
        return (String) this.redisTemplate.execute(new RedisCallback() {
            @Override
            public String doInRedis(RedisConnection redisConnection) throws DataAccessException {
                return new String(redisConnection.get(key.getBytes()));
            }
        });
    }

    @Override
    public String set(String key, String value) {
        return (String)this.redisTemplate.execute(new RedisCallback() {
            @Override
            public Void doInRedis(RedisConnection redisConnection) throws DataAccessException {
                redisConnection.set(key.getBytes(), value.getBytes());
                return null;
            }
        });
    }

    @Override
    public Long increase(String key) {
        return (Long) this.redisTemplate.execute(new RedisCallback() {
            @Override
            public Long doInRedis(RedisConnection redisConnection) throws DataAccessException {
                return redisConnection.incr(key.getBytes());
            }
        });
    }

    @Override
    public Set<String> memberOfSet(final String key) {
        Set<byte[]> members = memberOfSet(key.getBytes());
        return members.
                stream().
                map(member -> new String(member)).
                collect(Collectors.toSet());
    }

    public Set<byte[]> memberOfSet(final byte[] key) {
        return (Set<byte[]>)this.redisTemplate.execute(new RedisCallback() {
            @Override
            public Set<byte[]> doInRedis(RedisConnection redisConnection) throws DataAccessException {
                return redisConnection.sMembers(key);
            }
        });
    }

    @Override
    public String leftPopAndPush(String popKey, String pushKey) {
        List keys = new ArrayList<>();
        keys.add(popKey);
        keys.add(pushKey);
        return (String) this.redisTemplate.execute(this.lpoplpushScriptHash.get(),
                keys, null);
    }

    @Override
    public String pop(String queueKey, String inFlightKey, String freqKey, String now) {
        List keys = new ArrayList<>();
        keys.add(queueKey);
        keys.add(inFlightKey);
        keys.add(freqKey);
        return (String) this.redisTemplate.execute(this.popScriptHash.get(),
                keys, now);
    }
}
