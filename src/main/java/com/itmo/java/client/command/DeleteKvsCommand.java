package com.itmo.java.client.command;

import com.itmo.java.protocol.model.RespArray;
import com.itmo.java.protocol.model.RespBulkString;
import com.itmo.java.protocol.model.RespCommandId;

public class DeleteKvsCommand implements KvsCommand {
    private static final String COMMAND_NAME = "DELETE_KEY";
    private final String databaseName;
    private final String tableName;
    private final String key;
    private final int id;


    public DeleteKvsCommand(String databaseName, String tableName, String key) {
        this.databaseName = databaseName;
        this.tableName = tableName;
        this.key = key;
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
        RespBulkString keyResp = new RespBulkString(key.getBytes());
        RespArray dataResp = new RespArray(commandIdResp, commandNameResp, databaseNameResp, tableNameResp, keyResp);
        return dataResp;
    }

    @Override
    public int getCommandId() {
        return id;
    }
}
