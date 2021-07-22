package com.itmo.java.client.command;

import com.itmo.java.protocol.model.RespArray;
import com.itmo.java.protocol.model.RespBulkString;
import com.itmo.java.protocol.model.RespCommandId;

/**
 * Команда для создания бд
 */
public class CreateDatabaseKvsCommand implements KvsCommand {
    private static final String COMMAND_NAME = "CREATE_DATABASE";
    private final String databaseName;
    private final int id;

    /**
     * Создает объект
     *
     * @param databaseName имя базы данных
     */
    public CreateDatabaseKvsCommand(String databaseName) {
        this.databaseName = databaseName;
        this.id = idGen.incrementAndGet();
    }

    /**
     * Возвращает RESP объект. {@link RespArray} с {@link RespCommandId}, именем команды, аргументами в виде {@link RespBulkString}
     *
     * @return объект
     */
    @Override
    public RespArray serialize() {
        RespCommandId commandIdResp = new RespCommandId(id);
        RespBulkString commandNameResp = new RespBulkString(COMMAND_NAME.getBytes());
        RespBulkString databaseNameResp = new RespBulkString(databaseName.getBytes());
        RespArray dataResp = new RespArray(commandIdResp, commandNameResp, databaseNameResp);
        return dataResp;
    }

    @Override
    public int getCommandId() {
        return id;
    }
}
