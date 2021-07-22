package com.itmo.java.basics.logic.io;

import com.itmo.java.basics.logic.WritableDatabaseRecord;

import java.io.*;
import java.nio.ByteBuffer;

/**
 * Записывает данные в БД
 */
public class DatabaseOutputStream extends DataOutputStream {
    public DatabaseOutputStream(OutputStream outputStream) {
        super(outputStream);
    }

    /**
     * Записывает в БД в следующем формате:
     * - Размер ключа в байтах используя {@link WritableDatabaseRecord#getKeySize()}
     * - Ключ
     * - Размер записи в байтах {@link WritableDatabaseRecord#getValueSize()}
     * - Запись
     * Например при использовании UTF_8,
     * "key" : "value"
     * 3key5value
     * Метод вернет 10
     *
     * @param databaseRecord запись
     * @return размер записи
     * @throws IOException если запись не удалась
     */
    public int write(WritableDatabaseRecord databaseRecord) throws IOException {
        byte[] data = new byte[(int) databaseRecord.size()];
        byte[] keySizeByteArray = ByteBuffer.allocate(4).putInt(databaseRecord.getKeySize()).array();
        byte[] valueSizeByteArray = ByteBuffer.allocate(4).putInt(databaseRecord.getValueSize()).array();

        ByteBuffer buff = ByteBuffer.wrap(data);
        buff.put(keySizeByteArray);
        buff.put(databaseRecord.getKey());
        buff.put(valueSizeByteArray);
        if (databaseRecord.getValueSize() > 0) {
            buff.put(databaseRecord.getValue());
        }

        data = buff.array();

        out.write(data);
        return data.length;
    }
}