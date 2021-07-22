package com.itmo.java.basics.logic.impl;

import com.itmo.java.basics.exceptions.DatabaseException;
import com.itmo.java.basics.initialization.SegmentInitializationContext;
import com.itmo.java.basics.logic.Segment;
import com.itmo.java.basics.exceptions.DatabaseException;
import com.itmo.java.basics.index.SegmentOffsetInfo;
import com.itmo.java.basics.index.impl.SegmentIndex;
import com.itmo.java.basics.index.impl.SegmentOffsetInfoImpl;
import com.itmo.java.basics.logic.DatabaseRecord;
import com.itmo.java.basics.logic.WritableDatabaseRecord;
import com.itmo.java.basics.logic.io.DatabaseInputStream;
import com.itmo.java.basics.logic.io.DatabaseOutputStream;
import lombok.EqualsAndHashCode;
import java.io.*;

import java.io.IOException;
import java.nio.file.Path;
import java.util.Optional;

/**
 * Сегмент - append-only файл, хранящий пары ключ-значение, разделенные специальным символом.
 * - имеет ограниченный размер, большие значения (>100000) записываются в последний сегмент, если он не read-only
 * - при превышении размера сегмента создается новый сегмент и дальнейшие операции записи производятся в него
 * - именование файла-сегмента должно позволять установить очередность их появления
 * - является неизменяемым после появления более нового сегмента
 */
@EqualsAndHashCode
public class SegmentImpl implements Segment {
    private final String segmentName;
    private final Path segmentPath;
    private final SegmentIndex segmentIndex;
    private long segmentSize = 0;
    private final long maxSegmentBytesCount = 100_000;
    private boolean isReadOnly = false;

    private SegmentImpl(String segmentName, Path segmentPath, SegmentIndex segmentIndex) {
        this.segmentName = segmentName;
        this.segmentPath = segmentPath;
        this.segmentIndex = segmentIndex;
    }

    public static Segment create(String segmentName, Path tableRootPath) throws DatabaseException {
        File segmentFile = new File(tableRootPath.toAbsolutePath().toString(), segmentName);
        boolean fileCreated;
        try {
            fileCreated = segmentFile.createNewFile();
        } catch (IOException e) {
            throw new DatabaseException("Can't create file. segmentName = " + segmentName + "tableRootPath = " + tableRootPath.toString(), e);
        }
        if (!fileCreated) {
            throw new DatabaseException("segmentFile.createNewFile returns false");
        }

        Path segmentPath = segmentFile.toPath();
        Segment segment = new SegmentImpl(segmentName, segmentPath, new SegmentIndex());

        return segment;
    }

    public static Segment initializeFromContext(SegmentInitializationContext context) {
        SegmentImpl initializedSegment = new SegmentImpl(context.getSegmentName(), context.getSegmentPath(), context.getIndex());
        initializedSegment.segmentSize = context.getCurrentSize();

        if(initializedSegment.checkIsOverloaded()){
            initializedSegment.isReadOnly = true;
        }

        return initializedSegment;
    }

    private boolean checkIsOverloaded() {
        return segmentSize >= maxSegmentBytesCount;
    }

    static String createSegmentName(String tableName) {
        return tableName + "_" + System.currentTimeMillis();
    }

    @Override
    public String getName() {
        return segmentName;
    }

    @Override
    public boolean write(String objectKey, byte[] objectValue) throws IOException {
        if (isReadOnly) {
            return false;
        }

        byte[] key = objectKey.getBytes();
        WritableDatabaseRecord record;

        if (objectValue == null) {
            record = new RemoveDatabaseRecord(key);
        } else {
            record = new SetDatabaseRecord(key, objectValue);
        }

        try (FileOutputStream fileOutputStream = new FileOutputStream(String.valueOf(segmentPath), true);
             DatabaseOutputStream outputStream = new DatabaseOutputStream(fileOutputStream)) {
            if (segmentSize + record.size() >= maxSegmentBytesCount) {
                isReadOnly = true;
            }

            segmentIndex.onIndexedEntityUpdated(objectKey, new SegmentOffsetInfoImpl(segmentSize));

            int bytesWritten = outputStream.write(record);

            outputStream.close();

            segmentSize += bytesWritten;
        }

        return true;
    }

    @Override
    public Optional<byte[]> read(String objectKey) throws IOException {
        FileInputStream fileInputStream = new FileInputStream(String.valueOf(segmentPath));

        long offset;
        Optional<SegmentOffsetInfo> segmentOffsetInfo = segmentIndex.searchForKey(objectKey);
        if (segmentOffsetInfo.isEmpty()) {
            return Optional.empty();
        }

        offset = segmentOffsetInfo.get().getOffset();

        long skip = fileInputStream.skip(offset);
        if (skip != offset) {
            throw new IOException("Amount of skipped bytes is wrong. Skipped bytes = " + skip + " but offset = " + offset);
        }

        DatabaseInputStream databaseInputStream = new DatabaseInputStream(fileInputStream);

        Optional<DatabaseRecord> databaseRecord = databaseInputStream.readDbUnit();

        if (databaseRecord.isEmpty()) {
            return Optional.empty();
        } else {
            if (databaseRecord.get().getValue() == null)
                return Optional.empty();
            if (new String(databaseRecord.get().getValue()).equals("")) {
                return Optional.of(new byte[0]);
            } else {
                return Optional.of(databaseRecord.get().getValue());
            }
        }
    }

    @Override
    public boolean isReadOnly() {
        return isReadOnly;
    }

    @Override
    public boolean delete(String objectKey) throws IOException {
        byte[] key = objectKey.getBytes();
        RemoveDatabaseRecord removeDatabaseRecord = new RemoveDatabaseRecord(key);
        segmentIndex.onIndexedEntityUpdated(objectKey, null);

        int bytesWritten;
        try (FileOutputStream fileOutputStream = new FileOutputStream(String.valueOf(segmentPath), true);
             DatabaseOutputStream outputStream = new DatabaseOutputStream(fileOutputStream)) {
            if (segmentSize + removeDatabaseRecord.size() >= maxSegmentBytesCount) {
                isReadOnly = true;
            }
            segmentIndex.onIndexedEntityUpdated(objectKey, new SegmentOffsetInfoImpl(segmentSize));

            bytesWritten = outputStream.write(removeDatabaseRecord);

            outputStream.close();

            segmentSize += bytesWritten;
        }

        return bytesWritten > 0;
    }
}
