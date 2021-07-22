package com.itmo.java.basics.initialization.impl;

import com.itmo.java.basics.exceptions.DatabaseException;
import com.itmo.java.basics.index.impl.SegmentIndex;
import com.itmo.java.basics.initialization.InitializationContext;
import com.itmo.java.basics.initialization.Initializer;
import com.itmo.java.basics.initialization.SegmentInitializationContext;
import com.itmo.java.basics.logic.Table;
import com.itmo.java.basics.logic.impl.TableImpl;

import java.io.File;
import java.util.*;

public class TableInitializer implements Initializer {
    private final SegmentInitializer segmentInitializer;

    public TableInitializer(SegmentInitializer segmentInitializer) {
        this.segmentInitializer = segmentInitializer;
    }

    /**
     * Добавляет в контекст информацию об инициализируемой таблице.
     * Запускает инициализацию всех сегментов в порядке их создания (из имени)
     *
     * @param context контекст с информацией об инициализируемой бд, окружении, таблицы
     * @throws DatabaseException если в контексте лежит неправильный путь к таблице, невозможно прочитать содержимого папки,
     *  или если возникла ошибка ошибка дочерних инициализаторов
     */
    @Override
    public void perform(InitializationContext context) throws DatabaseException {
        if (context.currentDbContext() == null) {
            throw new DatabaseException("Database context can't be null");
        }

        File tableDirFile = new File(context.currentTableContext().getTablePath().toString());
        if (!tableDirFile.exists()) {
            boolean dirWasCreated = tableDirFile.mkdir();
        }

        if (!tableDirFile.exists()) {
            throw new DatabaseException(tableDirFile + "doesn't exist");
        }

        File[] files = tableDirFile.listFiles();
        if (files == null) {
            throw new DatabaseException("There is no files in  " + tableDirFile);
        }

        var segmentFiles = Arrays.asList(files);
        Collections.sort(segmentFiles);

        for (File segmentFile : segmentFiles) {

            SegmentInitializationContext segmentInitContext = new SegmentInitializationContextImpl(segmentFile.getName(),
                    segmentFile.toPath(), 0, new SegmentIndex());

            InitializationContext contextWithSegment = InitializationContextImpl.builder()
                    .executionEnvironment(context.executionEnvironment())
                    .currentDatabaseContext(context.currentDbContext())
                    .currentTableContext(context.currentTableContext())
                    .currentSegmentContext(segmentInitContext)
                    .build();

            segmentInitializer.perform(contextWithSegment);
        }

        Table table = TableImpl.initializeFromContext(context.currentTableContext());
        context.currentDbContext().addTable(table);

    }
}
