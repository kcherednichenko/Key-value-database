package com.itmo.java.basics.initialization.impl;

import com.itmo.java.basics.exceptions.DatabaseException;
import com.itmo.java.basics.index.impl.SegmentOffsetInfoImpl;
import com.itmo.java.basics.initialization.InitializationContext;
import com.itmo.java.basics.initialization.Initializer;
import com.itmo.java.basics.logic.DatabaseRecord;
import com.itmo.java.basics.logic.Segment;
import com.itmo.java.basics.logic.impl.SegmentImpl;
import com.itmo.java.basics.logic.io.DatabaseInputStream;

import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.util.HashSet;
import java.util.Optional;

public class SegmentInitializer implements Initializer {

    /**
     * Добавляет в контекст информацию об инициализируемом сегменте.
     * Составляет индекс сегмента
     * Обновляет инфу в индексе таблицы
     *
     * @param context контекст с информацией об инициализируемой бд и об окружении
     * @throws DatabaseException если в контексте лежит неправильный путь к сегменту, невозможно прочитать содержимое. Ошибка в содержании
     */
    @Override
    public void perform(InitializationContext context) throws DatabaseException {
        HashSet<String> keys = new HashSet<>();
        int pos = 0;

        try {
            DatabaseInputStream databaseInputStream = new DatabaseInputStream(new FileInputStream(context.currentSegmentContext().getSegmentPath().toFile()));
            while (databaseInputStream.available() > 0) {
                Optional<DatabaseRecord> optionalDatabaseRecord;

                try {
                    optionalDatabaseRecord = databaseInputStream.readDbUnit();
                } catch (IOException e) {
                    throw new DatabaseException("There is a problem with input/output", e);
                }

                if (optionalDatabaseRecord.isEmpty()) {
                    throw new DatabaseException(optionalDatabaseRecord + " is empty");
                }

                DatabaseRecord databaseRecord = optionalDatabaseRecord.get();
                String lastKey = new String(databaseRecord.getKey());
                context.currentSegmentContext().getIndex().onIndexedEntityUpdated(lastKey, new SegmentOffsetInfoImpl(pos));
                keys.add(lastKey);
                pos += databaseRecord.size();

            }

        } catch (FileNotFoundException e) {
            throw new DatabaseException("Can't find the file", e);
        } catch (IOException e) {
            throw new DatabaseException("There is a problem with input/output", e);
        }

        Segment segment = SegmentImpl.initializeFromContext(new SegmentInitializationContextImpl(
               context.currentSegmentContext().getSegmentName(),
               context.currentSegmentContext().getSegmentPath(),
               pos,
               context.currentSegmentContext().getIndex()
        ));

        keys.forEach(k -> context.currentTableContext().getTableIndex().onIndexedEntityUpdated(k, segment));
        context.currentTableContext().updateCurrentSegment(segment);
    }
}
