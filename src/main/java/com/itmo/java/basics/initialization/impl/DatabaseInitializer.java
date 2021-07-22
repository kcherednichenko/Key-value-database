package com.itmo.java.basics.initialization.impl;

import com.itmo.java.basics.exceptions.DatabaseException;
import com.itmo.java.basics.index.impl.TableIndex;
import com.itmo.java.basics.initialization.DatabaseInitializationContext;
import com.itmo.java.basics.initialization.InitializationContext;
import com.itmo.java.basics.initialization.Initializer;
import com.itmo.java.basics.initialization.TableInitializationContext;
import com.itmo.java.basics.logic.Database;
import com.itmo.java.basics.logic.impl.DatabaseImpl;

import java.io.File;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Arrays;
import java.util.List;
import java.util.stream.Collectors;

public class DatabaseInitializer implements Initializer {
    private final TableInitializer tableInitializer;

    public DatabaseInitializer(TableInitializer tableInitializer) {
        this.tableInitializer = tableInitializer;
    }

    /**
     * Добавляет в контекст информацию об инициализируемой бд.
     * Запускает инициализацию всех таблиц это базы
     *
     * @param initialContext контекст с информацией об инициализируемой бд и об окружении
     * @throws DatabaseException если в контексте лежит неправильный путь к базе, невозможно прочитать содержимого папки,
     *  или если возникла ошибка дочерних инициализаторов
     */
    @Override
    public void perform(InitializationContext initialContext) throws DatabaseException {
        DatabaseInitializationContext dbContext = initialContext.currentDbContext();

        File dbDir = new File(dbContext.getDatabasePath().getParent().toString(), dbContext.getDbName());
        if (!dbDir.exists()) {
            boolean dirWasCreated = dbDir.mkdir();
        }

        if (!dbDir.exists()) {
            throw new DatabaseException(dbDir + "doesn't exist");
        }

        if(!dbDir.isDirectory()){
            throw new DatabaseException(dbDir + "is not a directory");
        }

        File[] files = dbDir.listFiles();
        if (files == null) {
            throw new DatabaseException("There is no files in " + dbDir);
        }

        if (initialContext.executionEnvironment() == null ) {
            throw new DatabaseException("executionEnvironment can't be null");
        }

        List<File> tableDirs = Arrays.stream(files)
                .filter(File::isDirectory)
                .collect(Collectors.toList());

        for (File tableDir : tableDirs) {
            TableInitializationContext tableInitContext = new TableInitializationContextImpl(tableDir.getName(), dbDir.toPath(), new TableIndex());

            InitializationContext contextWithTable = InitializationContextImpl.builder()
                    .executionEnvironment(initialContext.executionEnvironment())
                    .currentDatabaseContext(dbContext)
                    .currentTableContext(tableInitContext)
                    .build();

            tableInitializer.perform(contextWithTable);
        }

        Database db = DatabaseImpl.initializeFromContext(dbContext);

        initialContext.executionEnvironment().addDatabase(db);
    }
}
