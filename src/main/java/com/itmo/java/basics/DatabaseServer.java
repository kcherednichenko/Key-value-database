package com.itmo.java.basics;

import com.itmo.java.basics.console.*;
import com.itmo.java.basics.exceptions.DatabaseException;
import com.itmo.java.basics.initialization.InitializationContext;
import com.itmo.java.basics.initialization.impl.*;
import com.itmo.java.protocol.model.RespArray;
import com.itmo.java.protocol.model.RespObject;

import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

public class DatabaseServer {

    private ExecutorService executorService = Executors.newSingleThreadExecutor();
    private final ExecutionEnvironment env;
    private final DatabaseServerInitializer initializer;

    public DatabaseServer(ExecutionEnvironment env, DatabaseServerInitializer initializer) {
        this.env = env;
        this.initializer = initializer;
    }

    /**
     * Constructor
     * Конструктор
     *
     * @param env         env для инициализации. Далее работа происходит с заполненным объектом
     * @param initializer готовый чейн инициализации
     * @throws DatabaseException если произошла ошибка инициализации
     */
    public static DatabaseServer initialize(ExecutionEnvironment env, DatabaseServerInitializer initializer) throws DatabaseException {
        InitializationContext context = InitializationContextImpl
                .builder()
                .executionEnvironment(env)
                .build();

        initializer.perform(context);

        DatabaseServer databaseServer = new DatabaseServer(env, initializer);

        return databaseServer;
    }

    public CompletableFuture<DatabaseCommandResult> executeNextCommand(RespArray message) {
        return CompletableFuture.supplyAsync(() -> {
            List<RespObject> objects = message.getObjects();
            String commandName = objects.get(DatabaseCommandArgPositions.COMMAND_NAME.getPositionIndex()).asString();
            DatabaseCommand databaseCommand = DatabaseCommands.valueOf(commandName).getCommand(env, objects);
            DatabaseCommandResult result = databaseCommand.execute();

            return result;

        }, executorService);
    }

    public CompletableFuture<DatabaseCommandResult> executeNextCommand(DatabaseCommand command) {
        return CompletableFuture.supplyAsync(command::execute, executorService);
    }

    public ExecutionEnvironment getEnv() {
        return env;
    }
}