package com.itmo.java.basics.console.impl;

import com.itmo.java.basics.console.*;
import com.itmo.java.basics.exceptions.DatabaseException;
import com.itmo.java.basics.logic.Database;
import com.itmo.java.basics.logic.DatabaseFactory;
import com.itmo.java.protocol.model.RespObject;

import java.util.List;

/**
 * Команда для создания базы данных
 */
public class CreateDatabaseCommand implements DatabaseCommand {

    private ExecutionEnvironment env;
    private final DatabaseFactory factory;
    private final List<RespObject> commandArgs;
    private final String commandId;
    private final String dbName;

    /**
     * Создает команду.
     * <br/>
     * Обратите внимание, что в конструкторе нет логики проверки валидности данных. Не проверяется, можно ли исполнить команду. Только формальные признаки (например, количество переданных значений или ненуловость объектов
     *
     * @param env         env
     * @param factory     функция создания базы данных (пример: DatabaseImpl::create)
     * @param commandArgs аргументы для создания (порядок - {@link DatabaseCommandArgPositions}.
     *                    Id команды, имя команды, имя создаваемой бд
     * @throws IllegalArgumentException если передано неправильное количество аргументов
     */
    public CreateDatabaseCommand(ExecutionEnvironment env, DatabaseFactory factory, List<RespObject> commandArgs) {
        this.env = env;

        if (commandArgs.size() != 3) {
            throw new IllegalArgumentException("Wrong amount of arguments: " + commandArgs.size() + ". Should be 3");
        }

        RespObject commandIdRespObject = commandArgs.get(DatabaseCommandArgPositions.COMMAND_ID.getPositionIndex());
        this.commandId = commandIdRespObject.asString();

        RespObject commandRespObject = commandArgs.get(DatabaseCommandArgPositions.COMMAND_NAME.getPositionIndex());
        String commandName = commandRespObject.asString();

        RespObject dbRespObject = commandArgs.get(DatabaseCommandArgPositions.DATABASE_NAME.getPositionIndex());
        this.dbName = dbRespObject.asString();

        this.commandArgs = commandArgs;
        this.factory = factory;
    }

    /**
     * Создает бд в нужном env
     *
     * @return {@link DatabaseCommandResult#success(byte[])} с сообщением о том, что заданная база была создана. Например, "Database db1 created"
     */
    @Override
    public DatabaseCommandResult execute() {
        Database database = null;
        try {
            database = factory.createNonExistent(dbName, env.getWorkingPath());
            env.addDatabase(database);
            return DatabaseCommandResult.success(("Database " + dbName + " was created").getBytes());
        } catch (DatabaseException e) {
            return DatabaseCommandResult.error(e);
        }
    }
}
