package com.itmo.java.basics.logic.io;

import com.itmo.java.basics.logic.DatabaseRecord;
import com.itmo.java.basics.logic.WritableDatabaseRecord;
import com.itmo.java.basics.logic.impl.RemoveDatabaseRecord;
import com.itmo.java.basics.logic.impl.SetDatabaseRecord;

import java.io.DataInputStream;
import java.io.EOFException;
import java.io.IOException;
import java.io.InputStream;
import java.nio.BufferUnderflowException;
import java.nio.ByteBuffer;
import java.util.Optional;

/**
 * Класс, отвечающий за чтение данных из БД
 */
public class DatabaseInputStream extends DataInputStream {
    private static final int REMOVED_OBJECT_SIZE = -1;
    private byte[] lastKeyBytes;
    private long read = 0;

    public DatabaseInputStream(InputStream inputStream) {
        super(inputStream);
    }

    /**
     * Читает следующую запись (см {@link DatabaseOutputStream#write(WritableDatabaseRecord)})
     *
     * @return следующую запись, если она существует. {@link Optional#empty()} - если конец файла достигнут
     */
    public Optional<DatabaseRecord> readDbUnit() throws IOException {
        try {
            byte[] keySizeBytes = in.readNBytes(4);
            ByteBuffer buff = ByteBuffer.wrap(keySizeBytes);
            int keySize = buff.getInt();

            byte[] keyBytes = in.readNBytes(keySize);
            lastKeyBytes = keyBytes;

            byte[] valueSizeBytes = in.readNBytes(4);
            buff = ByteBuffer.wrap(valueSizeBytes);
            int valueSize = buff.getInt();

            read += 4 + keySize + 4;

            if (valueSize == REMOVED_OBJECT_SIZE) {
                RemoveDatabaseRecord removeDatabaseRecord = new RemoveDatabaseRecord(keyBytes);
                return Optional.of(removeDatabaseRecord);
            } else {
                byte[] valueBytes = in.readNBytes(valueSize);
                SetDatabaseRecord record = new SetDatabaseRecord(keyBytes, valueBytes);

                read += valueSize;
                return Optional.of(record);
            }

        } catch (IOException | BufferUnderflowException e) {
            return Optional.empty();
        }
    }
}
