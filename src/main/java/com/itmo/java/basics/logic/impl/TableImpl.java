package com.itmo.java.basics.logic.impl;

import com.itmo.java.basics.exceptions.DatabaseException;
import com.itmo.java.basics.index.impl.TableIndex;
import com.itmo.java.basics.logic.Segment;
import com.itmo.java.basics.initialization.TableInitializationContext;
import com.itmo.java.basics.logic.Table;
import lombok.EqualsAndHashCode;

import java.io.File;
import java.io.IOException;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.Optional;

/**
 * Таблица - логическая сущность, представляющая собой набор файлов-сегментов, которые объединены одним
 * именем и используются для хранения однотипных данных (данных, представляющих собой одну и ту же сущность,
 * например, таблица "Пользователи")
 * <p>
 * - имеет единый размер сегмента
 * - представляет из себя директорию в файловой системе, именованную как таблица
 * и хранящую файлы-сегменты данной таблицы
 */

@EqualsAndHashCode
public class TableImpl implements Table {
    private final String tableName;
    private final TableIndex tableIndex;
    private Path pathToDatabaseRoot;
    private Segment lastSegment;

    private TableImpl(String tableName, TableIndex tableIndex, Segment lastSegment, Path pathToDatabaseRoot) {
        this.tableName = tableName;
        this.tableIndex = tableIndex;
        this.lastSegment = lastSegment;
        this.pathToDatabaseRoot = pathToDatabaseRoot;
    }

    private TableImpl(String tableName, TableIndex tableIndex, Path pathToDatabaseRoot) {
        this.tableName = tableName;
        this.tableIndex = tableIndex;
        this.pathToDatabaseRoot = pathToDatabaseRoot;
    }

    private TableImpl(String tableName, TableIndex tableIndex, Segment lastSegment) {
        this.tableName = tableName;
        this.tableIndex = tableIndex;
        this.lastSegment = lastSegment;
    }

    public static Table create(String tableName, Path pathToDatabaseRoot, TableIndex tableIndex) throws DatabaseException {
        if (tableName == null) {
            throw new DatabaseException("tableName can't be null");
        }
        if (pathToDatabaseRoot == null) {
            throw new DatabaseException("pathToDatabaseRoot can't be null");
        }

        File tableDir = new File(pathToDatabaseRoot.toAbsolutePath().toString(), tableName);
        boolean dirCreated = tableDir.mkdir();
        if (!dirCreated) {
            throw new DatabaseException("tableDir.createNewFile returns false");
        }

        Path tablePath = tableDir.toPath();
        TableImpl table = new TableImpl(tableName, tableIndex, pathToDatabaseRoot);
        table.lastSegment = SegmentImpl.create(SegmentImpl.createSegmentName(tableName), tablePath);

        return new CachingTable(table);
    }

    public static Table initializeFromContext(TableInitializationContext context) {
        TableImpl table = new TableImpl(context.getTableName(), context.getTableIndex(), context.getCurrentSegment());
        table.pathToDatabaseRoot = context.getTablePath().getParent();
        return new CachingTable(table);
    }

    @Override
    public String getName() {
        return tableName;
    }

    @Override
    public void write(String objectKey, byte[] objectValue) throws DatabaseException {
        if (lastSegment.isReadOnly()) {
            lastSegment = SegmentImpl.create(SegmentImpl.createSegmentName(tableName), Paths.get(pathToDatabaseRoot.toString(), tableName));
        }

        try {
            lastSegment.write(objectKey, objectValue);
            tableIndex.onIndexedEntityUpdated(objectKey, lastSegment);
        } catch (IOException e) {
            throw new DatabaseException("Can't write in segment, there is a problem with input/output. Key = " + objectKey, e);
        }
    }

    @Override
    public Optional<byte[]> read(String objectKey) throws DatabaseException {
        try {
            Optional<Segment> optionalSegment = tableIndex.searchForKey(objectKey);
            if (optionalSegment.isPresent()) {
                Segment segment = optionalSegment.get();
                return segment.read(objectKey);
            } else {
                return Optional.empty();
            }
        } catch (IOException e) {
            throw new DatabaseException("There is problem with input/output. Key = " + objectKey, e);
        }
    }

    @Override
    public void delete(String objectKey) throws DatabaseException {
        Optional<Segment> optionalSegment = tableIndex.searchForKey(objectKey);
        if (optionalSegment.isPresent()) {
            Segment segment = optionalSegment.get();
            if (!segment.isReadOnly()) {
                try {
                    boolean isDeleted = segment.delete(objectKey);
                } catch (IOException e) {
                    throw new DatabaseException("There is problem with input/output. Key = " + objectKey, e);
                }
            } else {
                if (lastSegment.isReadOnly()) {
                    lastSegment = SegmentImpl.create(SegmentImpl.createSegmentName(tableName), Paths.get(pathToDatabaseRoot.toString(), tableName));
                }
                try {
                    lastSegment.write(objectKey, new byte[0]);
                    tableIndex.onIndexedEntityUpdated(objectKey, lastSegment);
                } catch (IOException e) {
                    throw new DatabaseException("There is problem with input/output. Key = " + objectKey, e);
                }
            }
        }
    }
}
