package com.itmo.java.client.client;


import com.itmo.java.client.command.*;
import com.itmo.java.client.connection.KvsConnection;
import com.itmo.java.client.exception.ConnectionException;
import com.itmo.java.client.exception.DatabaseExecutionException;
import com.itmo.java.protocol.model.RespObject;

import java.util.function.Supplier;

public class SimpleKvsClient implements KvsClient {

    private final Supplier<KvsConnection> connectionSupplier;
    private final String databaseName;

    /**
     * Конструктор
     *
     * @param databaseName       имя базы, с которой работает
     * @param connectionSupplier метод создания подключения к базе
     */
    public SimpleKvsClient(String databaseName, Supplier<KvsConnection> connectionSupplier) {
        this.databaseName = databaseName;
        this.connectionSupplier = connectionSupplier;
    }

    @Override
    public String createDatabase() throws DatabaseExecutionException {
        CreateDatabaseKvsCommand createCommand = new CreateDatabaseKvsCommand(databaseName);
        try {
            RespObject response = connectionSupplier.get().send(createCommand.getCommandId(), createCommand.serialize());
            if (response.isError()) {
                throw new DatabaseExecutionException(response.asString());
            }
            return response.asString();
        } catch (Exception e) {
            throw new DatabaseExecutionException("Error! There is a problem with connection");
        }
    }

    @Override
    public String createTable(String tableName) throws DatabaseExecutionException {
        CreateTableKvsCommand createCommand = new CreateTableKvsCommand(databaseName, tableName);
        try {
            RespObject response = connectionSupplier.get().send(createCommand.getCommandId(), createCommand.serialize());
            if (response.isError()) {
                throw new DatabaseExecutionException(response.asString());
            }
            return response.asString();
        } catch (ConnectionException e) {
            throw new DatabaseExecutionException("Error! There is a problem with connection");
        }
    }

    @Override
    public String get(String tableName, String key) throws DatabaseExecutionException {
        GetKvsCommand createGetCommand = new GetKvsCommand(databaseName, tableName, key);
        try {
            RespObject response = connectionSupplier.get().send(createGetCommand.getCommandId(), createGetCommand.serialize());
            if (response.isError()) {
                throw new DatabaseExecutionException(response.asString());
            }
            return response.asString();
        } catch (ConnectionException e) {
            throw new DatabaseExecutionException("Error! There is a problem with connection");
        }
    }

    @Override
    public String set(String tableName, String key, String value) throws DatabaseExecutionException {
        SetKvsCommand createSetCommand = new SetKvsCommand(databaseName, tableName, key, value);
        try {
            RespObject response = connectionSupplier.get().send(createSetCommand.getCommandId(), createSetCommand.serialize());
            if (response.isError()) {
                throw new DatabaseExecutionException(response.asString());
            }
            return response.asString();
        } catch (ConnectionException e) {
            throw new DatabaseExecutionException("Error! There is a problem with connection");
        }
    }

    @Override
    public String delete(String tableName, String key) throws DatabaseExecutionException {
        DeleteKvsCommand createDeleteCommand = new DeleteKvsCommand(databaseName, tableName, key);
        try {
            RespObject response = connectionSupplier.get().send(createDeleteCommand.getCommandId(), createDeleteCommand.serialize());
            if (response.isError()) {
                throw new DatabaseExecutionException(response.asString());
            }
            return response.asString();
        } catch (ConnectionException e) {
            throw new DatabaseExecutionException("Error! There is a problem with connection");
        }
    }
}
