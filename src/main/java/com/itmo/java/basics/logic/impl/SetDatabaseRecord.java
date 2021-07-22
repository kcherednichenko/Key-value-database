package com.itmo.java.basics.logic.impl;

import com.itmo.java.basics.logic.WritableDatabaseRecord;
import lombok.EqualsAndHashCode;

/**
 * Запись в БД, означающая добавление значения по ключу
 */
@EqualsAndHashCode
public class SetDatabaseRecord implements WritableDatabaseRecord {
    private final byte[] key;
    private final byte[] value;

    public SetDatabaseRecord(byte[] key, byte[] value) {
        this.key = key;
        this.value = value;
    }

    @Override
    public int getKeySize() {
        return key.length;
    }

    @Override
    public int getValueSize() {
        return value.length;
    }

    @Override
    public byte[] getKey() {
        return key;
    }

    @Override
    public byte[] getValue() {
        return value;
    }

    @Override
    public long size() {
        return 4 + getKeySize() + 4 + getValueSize();
    }

    @Override
    public boolean isValuePresented() {
        return getValueSize() != 0;
    }
}
