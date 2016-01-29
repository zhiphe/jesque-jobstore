package com.jinfuzi.fund.jobqueue.jobstore;

import java.util.Set;

/**
 * Created by kevinhe on 16/1/27.
 */
public interface JobStore {
    String getNameSpace();
    void initialize() throws Exception;
    boolean ensureConnection();
    String disconnect();
    boolean reconnect(final int reconAttempts, final long reconnectSleepTime);
    String authenticate();
    String select();
    boolean canUseAsDelayedQueue(String key);
    Long rightPush(String key, String... strings);
    String leftPop(String key);
    Long addToSet(String key, String... members);
    Long addToSortedSet(String key, double score, String member);
    Long removeFromSet(String key, String... members);
    Long removeFromSortedSet(String key, String value);
    Long expire(String key, int seconds);
    Long delete(String key);
    Long delete(String... keys);
    Long setIfNotExist(String key, String value);
    Long leftTime(String key);
    Boolean contains(String key);
    String get(String key);
    String set(String key, String value);
    Long increase(String key);
    Set<String> memberOfSet(String key);
    String leftPopAndPush(String popKey, String pushKey);
    String pop(String queueKey, String inFlightKey, String freqKey, String now);
    boolean canUseAsRecurringQueue(String queueKey, String hashKey);
    void addRecurringJob(String queueKey, long future, String hashKey, String jobJson, long frequency);
    void removeRecurringJob(String queueKey, String hashKey, String jobJson);
}
