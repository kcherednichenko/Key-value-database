package com.itmo.java.basics.initialization.impl;

import com.itmo.java.basics.exceptions.DatabaseException;
import com.itmo.java.basics.initialization.DatabaseInitializationContext;
import com.itmo.java.basics.initialization.InitializationContext;
import com.itmo.java.basics.initialization.Initializer;

import java.io.File;
import java.util.Arrays;
import java.util.List;
import java.util.stream.Collectors;

public class DatabaseServerInitializer implements Initializer {
    private final DatabaseInitializer databaseInitializer;

    public DatabaseServerInitializer(DatabaseInitializer databaseInitializer) {
        this.databaseInitializer = databaseInitializer;
    }

    /**
     * Если заданная в окружении директория не существует - создает ее
     * Добавляет информацию о существующих в директории базах, начинает их инициализацию
     *
     * @param context контекст, содержащий информацию об окружении
     * @throws DatabaseException если произошла ошибка при создании директории, ее обходе или ошибка инициализации бд
     */
    @Override
    public void perform(InitializationContext context) throws DatabaseException {
        try {
            File rootDir = new File(context.executionEnvironment().getWorkingPath().toString());
            if (!rootDir.exists()) {
                boolean dirWasCreated = rootDir.mkdir();
            }

            if (!rootDir.exists()) {
                throw new DatabaseException(rootDir + "doesn't exist");
            }

            if(!rootDir.isDirectory()){
                throw new DatabaseException(rootDir + "is not a directory");
            }

            File[] files = rootDir.listFiles();
            if (files == null) {
                throw new DatabaseException("There is no files in " + rootDir);
            }

            List<File> databaseDirs = Arrays.stream(files)
                    .filter(File::isDirectory)
                    .collect(Collectors.toList());

            for(File databaseDir : databaseDirs) {
                DatabaseInitializationContext databaseInitContext = new DatabaseInitializationContextImpl(databaseDir.getName(), context.executionEnvironment().getWorkingPath());

                InitializationContextImpl contextWithDatabase = InitializationContextImpl.builder()
                        .executionEnvironment(context.executionEnvironment())
                        .currentDatabaseContext(databaseInitContext)
                        .build();

                databaseInitializer.perform(contextWithDatabase);
            }
        } catch (NullPointerException e) {
            throw new DatabaseException("rootDir can't be null");
        }
    }
}
