package com.itmo.java.basics.logic.impl;

import com.itmo.java.basics.logic.DatabaseCache;
import lombok.EqualsAndHashCode;

import java.util.LinkedHashMap;
import java.util.Map;

@EqualsAndHashCode
public class DatabaseCacheImpl implements DatabaseCache {
    private static final int CAPACITY = 5_000;

    private final LRU<String, byte[]> LRUCache;

    public DatabaseCacheImpl() {
        this.LRUCache = new LRU<>();
    }

    public DatabaseCacheImpl(int initialCapacity) {
        this.LRUCache = new LRU<>(initialCapacity);
    }

    @Override
    public byte[] get(String key) {
        return LRUCache.get(key);
    }

    @Override
    public void set(String key, byte[] value) {
        LRUCache.put(key, value);
    }

    @Override
    public void delete(String key) {
        LRUCache.remove(key);
    }

    @EqualsAndHashCode
    public static class LRU<K, V> extends LinkedHashMap<K, V> {
        private final int capacity;

        public LRU() {
            this.capacity = 5000;
        }

        public LRU(int initialCapacity) {
            super(initialCapacity, 1f, true);
            this.capacity = initialCapacity;
        }

        @Override
        protected boolean removeEldestEntry(Map.Entry<K, V> eldest) {
            return size() > capacity;
        }
    }
}
