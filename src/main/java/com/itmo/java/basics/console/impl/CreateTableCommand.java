package com.itmo.java.basics.console.impl;

import com.itmo.java.basics.console.*;
import com.itmo.java.basics.exceptions.DatabaseException;
import com.itmo.java.basics.logic.Database;
import com.itmo.java.protocol.model.RespObject;

import java.util.List;
import java.util.Optional;

/**
 * Команда для создания базы таблицы
 */
public class CreateTableCommand implements DatabaseCommand {

    private ExecutionEnvironment env;
    private final List<RespObject> commandArgs;
    private final String commandId;
    private final String dbName;
    private final String tableName;

    /**
     * Создает команду
     * <br/>
     * Обратите внимание, что в конструкторе нет логики проверки валидности данных. Не проверяется, можно ли исполнить команду. Только формальные признаки (например, количество переданных значений или ненуловость объектов
     *
     * @param env         env
     * @param commandArgs аргументы для создания (порядок - {@link DatabaseCommandArgPositions}.
     *                    Id команды, имя команды, имя бд, имя таблицы
     * @throws IllegalArgumentException если передано неправильное количество аргументов
     */
    public CreateTableCommand(ExecutionEnvironment env, List<RespObject> commandArgs) {
        this.env = env;

        if (commandArgs.size() != 4) {
            throw new IllegalArgumentException("Wrong amount of arguments: " + commandArgs.size() + ". Should be 6");
        }

        RespObject commandIdRespObject = commandArgs.get(DatabaseCommandArgPositions.COMMAND_ID.getPositionIndex());
        this.commandId = commandIdRespObject.asString();

        RespObject commandRespObject = commandArgs.get(DatabaseCommandArgPositions.COMMAND_NAME.getPositionIndex());
        String commandName = commandRespObject.asString();

        RespObject dbRespObject = commandArgs.get(DatabaseCommandArgPositions.DATABASE_NAME.getPositionIndex());
        this.dbName = dbRespObject.asString();

        RespObject tableRespObject = commandArgs.get(DatabaseCommandArgPositions.TABLE_NAME.getPositionIndex());
        this.tableName = tableRespObject.asString();

        this.commandArgs = commandArgs;
    }

    /**
     * Создает таблицу в нужной бд
     *
     * @return {@link DatabaseCommandResult#success(byte[])} с сообщением о том, что заданная таблица была создана. Например, "Table table1 in database db1 created"
     */
    @Override
    public DatabaseCommandResult execute() {
        Optional<Database> optionalDatabase = env.getDatabase(dbName);
        if (optionalDatabase.isEmpty()) {
            return DatabaseCommandResult.error("Error! optionalDatabase can't be null");
        } else {
            Database database = optionalDatabase.get();

            try {
                database.createTableIfNotExists(tableName);
                return DatabaseCommandResult.success(("Table: " + tableName + " in database: " + dbName + " is created").getBytes());
            } catch (DatabaseException e) {
                return DatabaseCommandResult.error(e);
            }
        }
    }
}
