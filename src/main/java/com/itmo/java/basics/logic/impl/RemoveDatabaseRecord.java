package com.itmo.java.basics.logic.impl;

import com.itmo.java.basics.logic.WritableDatabaseRecord;
import lombok.EqualsAndHashCode;

/**
 * Запись в БД, означающая удаление значения по ключу
 */

@EqualsAndHashCode
public class RemoveDatabaseRecord implements WritableDatabaseRecord {
    private final byte[] key;

    public RemoveDatabaseRecord(byte[] key) {
        this.key = key;
    }

    @Override
    public int getKeySize() {
        return key.length;
    }

    @Override
    public int getValueSize() {
        return -1;
    }

    @Override
    public byte[] getKey() {
        return key;
    }

    @Override
    public byte[] getValue() {
        return null;
    }

    @Override
    public long size() {
        return 4 + getKeySize() + 4;
    }

    @Override
    public boolean isValuePresented() {
        return false;
    }
}
