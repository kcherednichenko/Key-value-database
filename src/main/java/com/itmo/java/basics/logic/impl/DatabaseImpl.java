package com.itmo.java.basics.logic.impl;

import com.itmo.java.basics.exceptions.DatabaseException;
import com.itmo.java.basics.index.impl.TableIndex;
import com.itmo.java.basics.initialization.DatabaseInitializationContext;
import com.itmo.java.basics.logic.Database;
import com.itmo.java.basics.logic.Table;
import lombok.EqualsAndHashCode;

import java.io.File;
import java.nio.file.Path;
import java.util.HashMap;
import java.util.Map;
import java.util.Optional;

@EqualsAndHashCode
public class DatabaseImpl implements Database {
    private final String dbName;
    private final Path databaseRoot;
    private final Path dbPath;
    private final Map<String, Table> allTables;

    public DatabaseImpl(String dbName, Path databaseRoot) throws DatabaseException {
       this(dbName, databaseRoot, new HashMap<>());
    }

    public DatabaseImpl(String dbName, Path databaseRoot,  Map<String, Table> allTables) throws DatabaseException {
        this.dbName = dbName;
        this.databaseRoot = databaseRoot;
        this.allTables = allTables;

        File dbDir = new File(databaseRoot.toAbsolutePath().toString(), dbName);
        if(!dbDir.exists()) {
            boolean dirCreated = dbDir.mkdirs();
            if (!dirCreated) {
                throw new DatabaseException("dbDir.createNewFile returns false");
            }
        }

        dbPath = dbDir.toPath();
    }

    /**
     * @param databaseRoot путь к директории, которая может содержать несколько БД,
     *                     поэтому при создании БД необходимо создать директорию внутри databaseRoot.
     */
    public static Database create(String dbName, Path databaseRoot) throws DatabaseException {
        DatabaseImpl database;
        if (dbName == null) {
            throw new DatabaseException("dbName can't be null");
        }
        if (databaseRoot == null) {
            throw new DatabaseException("databaseRoot can't be null");
        }
        database = new DatabaseImpl(dbName, databaseRoot);
        return database;
    }

    public static Database initializeFromContext(DatabaseInitializationContext context) {
        DatabaseImpl initializedDatabase;
        try {
            initializedDatabase = new DatabaseImpl(context.getDbName(), context.getDatabasePath().getParent(), context.getTables());
        } catch (DatabaseException e) {
            throw new RuntimeException(e);
        }
        return initializedDatabase;
    }

    @Override
    public String getName() {
        return dbName;
    }

    @Override
    public void createTableIfNotExists(String tableName) throws DatabaseException {
        if (!allTables.containsKey(tableName)) {
            TableIndex newTableIndex = new TableIndex();
            Table newTable = TableImpl.create(tableName, dbPath, newTableIndex);
            allTables.put(tableName, newTable);
        } else {
            throw new DatabaseException("Can't create table because database already contains table with that name: " + tableName);
        }
    }

    @Override
    public void write(String tableName, String objectKey, byte[] objectValue) throws DatabaseException {
        if (allTables.containsKey(tableName)) {
            Table table = allTables.get(tableName);
            table.write(objectKey, objectValue);
        } else {
            throw new DatabaseException("There is no such table: " + tableName);
        }
    }

    @Override
    public Optional<byte[]> read(String tableName, String objectKey) throws DatabaseException {
        if (allTables.containsKey(tableName)) {
            Table table = allTables.get(tableName);
            Optional<byte[]> value = table.read(objectKey);
            return value;
        } else {
            throw new DatabaseException("There is no such table: " + tableName);
        }
    }

    @Override
    public void delete(String tableName, String objectKey) throws DatabaseException {
        if (allTables.containsKey(tableName)) {
            Table table = allTables.get(tableName);
            table.delete(objectKey);
        } else {
            throw new DatabaseException("There is no such table: " + tableName);
        }
    }
}
