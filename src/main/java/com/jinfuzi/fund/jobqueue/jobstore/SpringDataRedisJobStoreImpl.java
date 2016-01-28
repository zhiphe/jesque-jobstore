package com.jinfuzi.fund.jobqueue.jobstore;

import java.util.Set;

/**
 * Created by kevinhe on 16/1/28.
 */
public class SpringDataRedisJobStoreImpl implements JobStore {
    @Override
    public void initialize() throws Exception {

    }

    @Override
    public boolean ensureConnection() {
        return false;
    }

    @Override
    public String disconnect() {
        return null;
    }

    @Override
    public boolean reconnect(int reconAttempts, long reconnectSleepTime) {
        return false;
    }

    @Override
    public String authenticate(String password) {
        return null;
    }

    @Override
    public String select(int index) {
        return null;
    }

    @Override
    public Long rightPush(String key, String... strings) {
        return null;
    }

    @Override
    public String leftPop(String key) {
        return null;
    }

    @Override
    public Long addToSet(String key, String... members) {
        return null;
    }

    @Override
    public Long removeFromSet(String key, String... members) {
        return null;
    }

    @Override
    public Long expire(String key, int seconds) {
        return null;
    }

    @Override
    public Long delete(String key) {
        return null;
    }

    @Override
    public Long delete(String... keys) {
        return null;
    }

    @Override
    public Long setIfNotExist(String key, String value) {
        return null;
    }

    @Override
    public Long leftTime(String key) {
        return null;
    }

    @Override
    public Boolean contains(String key) {
        return null;
    }

    @Override
    public String get(String key) {
        return null;
    }

    @Override
    public String set(String key, String value) {
        return null;
    }

    @Override
    public Long increase(String key) {
        return null;
    }

    @Override
    public Set<String> memberOfSet(String key) {
        return null;
    }

    @Override
    public String leftPopAndPush(String popKey, String pushKey) {
        return null;
    }

    @Override
    public String pop(String queueKey, String inFlightKey, String freqKey, String now) {
        return null;
    }
}
