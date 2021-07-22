package com.itmo.java.basics.console.impl;

import com.itmo.java.basics.console.*;
import com.itmo.java.basics.exceptions.DatabaseException;
import com.itmo.java.basics.logic.Database;
import com.itmo.java.protocol.model.RespObject;

import java.util.List;
import java.util.Optional;

/**
 * Команда для создания удаления значения по ключу
 */
public class DeleteKeyCommand implements DatabaseCommand {

    private ExecutionEnvironment env;
    private final List<RespObject> commandArgs;
    private final String commandId;
    private final String dbName;
    private final String tableName;
    private final String key;

    /**
     * Создает команду.
     * <br/>
     * Обратите внимание, что в конструкторе нет логики проверки валидности данных. Не проверяется, можно ли исполнить команду. Только формальные признаки (например, количество переданных значений или ненуловость объектов
     *
     * @param env         env
     * @param commandArgs аргументы для создания (порядок - {@link DatabaseCommandArgPositions}.
     *                    Id команды, имя команды, имя бд, таблицы, ключ
     * @throws IllegalArgumentException если передано неправильное количество аргументов
     */
    public DeleteKeyCommand(ExecutionEnvironment env, List<RespObject> commandArgs) {
        this.env = env;

        if (commandArgs.size() != 5) {
            throw new IllegalArgumentException("Wrong amount of arguments: " + commandArgs.size() + ". Should be 5");
        }

        RespObject commandIdRespObject = commandArgs.get(DatabaseCommandArgPositions.COMMAND_ID.getPositionIndex());
        this.commandId = commandIdRespObject.asString();

        RespObject commandRespObject = commandArgs.get(DatabaseCommandArgPositions.COMMAND_NAME.getPositionIndex());
        String commandName = commandRespObject.asString();

        RespObject dbRespObject = commandArgs.get(DatabaseCommandArgPositions.DATABASE_NAME.getPositionIndex());
        this.dbName = dbRespObject.asString();

        RespObject tableRespObject = commandArgs.get(DatabaseCommandArgPositions.TABLE_NAME.getPositionIndex());
        this.tableName = tableRespObject.asString();


        RespObject keyRespObject = commandArgs.get(DatabaseCommandArgPositions.KEY.getPositionIndex());
        this.key = keyRespObject.asString();

        this.commandArgs = commandArgs;
    }

    /**
     * Удаляет значение по ключу
     *
     * @return {@link DatabaseCommandResult#success(byte[])} с удаленным значением. Например, "previous"
     */
    @Override
    public DatabaseCommandResult execute() {
        Optional<Database> optionalDatabase = env.getDatabase(dbName);
        if (optionalDatabase.isEmpty()) {
            return DatabaseCommandResult.error("Error! optionalDatabase can't be null");
        } else {
            Database database = optionalDatabase.get();
            try {
                Optional<byte[]> readBytesOptional = database.read(tableName, key);

                if (readBytesOptional.isPresent()) {
                    database.delete(tableName, key);
                    return DatabaseCommandResult.success(readBytesOptional.get());
                } else {
                    return DatabaseCommandResult.error("There is no such key: " + key);
                }
            } catch (DatabaseException e) {
                return DatabaseCommandResult.error(e);
            }
        }
    }
}
