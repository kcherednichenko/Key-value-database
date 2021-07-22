package com.itmo.java.basics.console.impl;

import com.itmo.java.basics.config.DatabaseConfig;
import com.itmo.java.basics.console.ExecutionEnvironment;
import com.itmo.java.basics.exceptions.DatabaseException;
import com.itmo.java.basics.logic.Database;
import com.itmo.java.basics.logic.impl.DatabaseImpl;

import java.io.File;
import java.nio.file.Path;
import java.util.Arrays;
import java.util.HashMap;
import java.util.Optional;

public class ExecutionEnvironmentImpl implements ExecutionEnvironment {
    private final DatabaseConfig config;
    private final HashMap<String, Database> databases;

    public ExecutionEnvironmentImpl(DatabaseConfig config) {
        this.config = config;
        databases = new HashMap<>();
    }

    @Override
    public Optional<Database> getDatabase(String name) {
        if (databases.containsKey(name))  {
            return Optional.of(databases.get(name));
        } else {
            return Optional.empty();
        }
    }

    @Override
    public void addDatabase(Database db) {
        databases.put(db.getName(), db);
    }

    @Override
    public Path getWorkingPath() {
        return Path.of(config.getWorkingPath());
    }
}
