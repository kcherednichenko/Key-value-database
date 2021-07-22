package com.itmo.java.client.command;

import com.itmo.java.protocol.model.RespArray;
import com.itmo.java.protocol.model.RespBulkString;
import com.itmo.java.protocol.model.RespCommandId;

/**
 * Команда для создания таблицы
 */
public class CreateTableKvsCommand implements KvsCommand {
    private static final String COMMAND_NAME = "CREATE_TABLE";
    private final String databaseName;
    private final String tableName;
    private final int id;

    public CreateTableKvsCommand(String databaseName, String tableName) {
        this.databaseName = databaseName;
        this.tableName = tableName;
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
        RespBulkString tableNameResp = new RespBulkString(tableName.getBytes());
        RespArray dataResp = new RespArray(commandIdResp, commandNameResp, databaseNameResp, tableNameResp);
        return dataResp;
    }

    @Override
    public int getCommandId() {
        return id;
    }
}
