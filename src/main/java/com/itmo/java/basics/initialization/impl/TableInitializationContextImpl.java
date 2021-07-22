package com.itmo.java.basics.initialization.impl;

import com.itmo.java.basics.index.impl.TableIndex;
import com.itmo.java.basics.initialization.TableInitializationContext;
import com.itmo.java.basics.logic.Segment;
import com.itmo.java.basics.logic.io.DatabaseInputStream;

import java.io.File;
import java.nio.file.Path;

public class TableInitializationContextImpl implements TableInitializationContext {
    private final String tableName;
    private final Path databasePath;
    private final Path tablePath;
    private final TableIndex tableIndex;
    private Segment segment;

    public TableInitializationContextImpl(String tableName, Path databasePath, TableIndex tableIndex) {
        this.tableName = tableName;
        this.databasePath = databasePath;
        this.tableIndex = tableIndex;
        tablePath = new File(String.valueOf(databasePath), tableName).toPath();
    }

    @Override
    public String getTableName() {
        return tableName;
    }

    @Override
    public Path getTablePath() {
        return tablePath;
    }

    @Override
    public TableIndex getTableIndex() {
        return tableIndex;
    }

    @Override
    public Segment getCurrentSegment() {
        return segment;
    }


    @Override
    public void updateCurrentSegment(Segment segment) {
        this.segment = segment;
    }
}
