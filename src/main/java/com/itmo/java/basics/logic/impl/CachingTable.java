package com.itmo.java.basics.logic.impl;

import com.itmo.java.basics.exceptions.DatabaseException;
import com.itmo.java.basics.logic.DatabaseCache;
import com.itmo.java.basics.logic.Table;
import lombok.EqualsAndHashCode;

import java.util.Optional;

@EqualsAndHashCode
/**
 * Декоратор для таблицы. Кэширует данные
 */
public class CachingTable implements Table {
    private final DatabaseCache dbCache;
    private final Table table;

    public CachingTable(Table table) {
        this.dbCache = new DatabaseCacheImpl();
        this.table = table;
    }

    @Override
    public String getName() {
        return table.getName();
    }

    @Override
    public void write(String objectKey, byte[] objectValue) throws DatabaseException {
        table.write(objectKey, objectValue);
        dbCache.set(objectKey, objectValue);
    }

    @Override
    public Optional<byte[]> read(String objectKey) throws DatabaseException {
        byte[] valueInCache = dbCache.get(objectKey);

        if (valueInCache == null) {
            Optional<byte[]> valueInTable = table.read(objectKey);

            if (valueInTable.isPresent()) {
                dbCache.set(objectKey, valueInTable.get());
            }

            return valueInTable;
        }

        return Optional.of(valueInCache);
    }

    @Override
    public void delete(String objectKey) throws DatabaseException {
        dbCache.delete(objectKey);
        table.delete(objectKey);
    }
}
