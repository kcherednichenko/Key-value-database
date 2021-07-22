package com.itmo.java.basics;

import com.itmo.java.basics.logic.impl.DatabaseImpl;

import java.io.File;
import com.itmo.java.basics.config.DatabaseConfig;
import com.itmo.java.basics.console.ExecutionEnvironment;
import com.itmo.java.basics.console.impl.ExecutionEnvironmentImpl;
import com.itmo.java.basics.exceptions.DatabaseException;
import com.itmo.java.basics.initialization.InitializationContext;
import com.itmo.java.basics.initialization.Initializer;
import com.itmo.java.basics.initialization.impl.*;
import com.itmo.java.basics.logic.Database;
import com.itmo.java.basics.logic.impl.DatabaseImpl;

import java.io.File;
import java.nio.charset.Charset;
import java.nio.charset.StandardCharsets;
import java.util.Optional;


public class Init {
    public static void main(String[] args) {
        DatabaseConfig databaseConfig = new DatabaseConfig("/Users/ekaterinacerednicenko/Test");
        ExecutionEnvironment executionEnvironment = new ExecutionEnvironmentImpl(databaseConfig);

        InitializationContextImpl context = InitializationContextImpl
                .builder()
                .executionEnvironment(executionEnvironment)
                .build();

        Initializer initializer =
                new DatabaseServerInitializer(
                        new DatabaseInitializer(
                                new TableInitializer(
                                        new SegmentInitializer())));
        try {
            initializer.perform(context);
            Optional<Database> dbName_test_1618778504385 = context.executionEnvironment().getDatabase("dbName_test_1618951025478");
            Database database = dbName_test_1618778504385.get();
            Optional<byte[]> val = database.read("testTable", "key");
            Optional<byte[]> val2 = database.read("testTable", "CITY");
            if(val.isPresent()) {
                String s = new String(val.get(), StandardCharsets.UTF_8);
                System.out.println(s);
            }
            System.out.println(context.currentDbContext().getDatabasePath());
        } catch (DatabaseException e) {
            e.printStackTrace();
        }

    }

}
