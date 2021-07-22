package com.itmo.java.basics.connector;

import com.itmo.java.basics.DatabaseServer;
import com.itmo.java.basics.config.ConfigLoader;
import com.itmo.java.basics.config.DatabaseServerConfig;
import com.itmo.java.basics.config.ServerConfig;
import com.itmo.java.basics.console.DatabaseCommandResult;
import com.itmo.java.basics.console.ExecutionEnvironment;
import com.itmo.java.basics.console.impl.ExecutionEnvironmentImpl;
import com.itmo.java.basics.initialization.impl.*;
import com.itmo.java.basics.resp.CommandReader;
import com.itmo.java.client.client.KvsClient;
import com.itmo.java.client.client.SimpleKvsClient;
import com.itmo.java.client.connection.ConnectionConfig;
import com.itmo.java.client.connection.SocketKvsConnection;
import com.itmo.java.protocol.RespReader;
import com.itmo.java.protocol.RespWriter;
import com.itmo.java.protocol.model.RespBulkString;

import java.io.Closeable;
import java.io.IOException;
import java.io.OutputStream;
import java.net.ServerSocket;
import java.net.Socket;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

/**
 * Класс, который предоставляет доступ к серверу через сокеты
 */
public class JavaSocketServerConnector implements Closeable {

    /**
     * Экзекьютор для выполнения ClientTask
     */
    private final ExecutorService clientIOWorkers = Executors.newSingleThreadExecutor();

    private final ServerSocket serverSocket;
    private final ExecutorService connectionAcceptorExecutor = Executors.newSingleThreadExecutor();
    private final DatabaseServer databaseServer;
    private final ServerConfig config;

    /**
     * Стартует сервер. По аналогии с сокетом открывает коннекшн в конструкторе.
     */
    public JavaSocketServerConnector(DatabaseServer databaseServer, ServerConfig config) throws IOException {
        this.databaseServer = databaseServer;
        this.config = config;
        this.serverSocket = new ServerSocket(config.getPort());
    }

    /**
     * Начинает слушать заданный порт, начинает аксептить клиентские сокеты. На каждый из них начинает клиентскую таску
     */
    public void start() {
        connectionAcceptorExecutor.submit(() -> {
            while (!Thread.interrupted()) {
                Socket clientSocket = null;
                try {
                    clientSocket = serverSocket.accept();
                    ClientTask clientTask = new ClientTask(clientSocket, databaseServer);
                    clientIOWorkers.submit(clientTask);
                } catch (IOException e) {
                    e.printStackTrace();
                }
            }
        });
    }

    /**
     * Закрывает все, что нужно ¯\_(ツ)_/¯
     */
    @Override
    public void close() {
        connectionAcceptorExecutor.shutdownNow();
        clientIOWorkers.shutdownNow();
        try {
            serverSocket.close();
        } catch (IOException e) {
            e.printStackTrace();
        }
    }


    public static void main(String[] args) throws Exception {
        ConfigLoader configLoader = new ConfigLoader();
        DatabaseServerConfig databaseServerConfig = configLoader.readConfig();
        ExecutionEnvironment executionEnvironment = new ExecutionEnvironmentImpl(databaseServerConfig.getDbConfig());

        DatabaseServerInitializer initializer =
                new DatabaseServerInitializer(
                        new DatabaseInitializer(
                                new TableInitializer(
                                        new SegmentInitializer())));

        DatabaseServer databaseServer = DatabaseServer.initialize(executionEnvironment, initializer);
        JavaSocketServerConnector javaSocketServerConnector = new JavaSocketServerConnector(databaseServer, databaseServerConfig.getServerConfig());
        javaSocketServerConnector.start();

        ConnectionConfig connectionConfig = new ConnectionConfig(databaseServerConfig.getServerConfig().getHost(), databaseServerConfig.getServerConfig().getPort());
        KvsClient kvsClient = new SimpleKvsClient("dbName_test_1623138948295", () -> new SocketKvsConnection(connectionConfig));
        String result = kvsClient.get("testTable", "KEK");
        System.out.println(result);
    }

    /**
     * Runnable, описывающий исполнение клиентской команды.
     */
    static class ClientTask implements Runnable, Closeable {
        private final Socket client;
        private final DatabaseServer server;
        private RespWriter writer;

        /**
         * @param client клиентский сокет
         * @param server сервер, на котором исполняется задача
         */
        public ClientTask(Socket client, DatabaseServer server) {
            this.client = client;
            this.server = server;
            try {
                OutputStream outputStream = client.getOutputStream();
                this.writer = new RespWriter(outputStream);
            } catch (IOException e) {
                e.printStackTrace();
            }
        }

        /**
         * Исполняет задачи из одного клиентского сокета, пока клиент не отсоединился или текущий поток не был прерван (interrupted).
         * Для кажной из задач:
         * 1. Читает из сокета команду с помощью {@link CommandReader}
         * 2. Исполняет ее на сервере
         * 3. Записывает результат в сокет с помощью {@link RespWriter}
         */
        @Override
        public void run() {
            try (CommandReader commandReader = new CommandReader(new RespReader(client.getInputStream()), server.getEnv())) {
                while (commandReader.hasNextCommand() && !Thread.currentThread().isInterrupted()) {
                    CompletableFuture<DatabaseCommandResult> commandResult = server.executeNextCommand(commandReader.readCommand());
                        DatabaseCommandResult result = commandResult.get();
                    if (result != null) {
                        writer.write(result.serialize());
                    } else {
                        writer.write(new RespBulkString(null));
                    }
                }
            } catch (Exception e) {
                throw new RuntimeException("Error! Execute command", e);
            } finally {
                close();
            }
        }

        /**
         * Закрывает клиентский сокет
         */
        @Override
        public void close() {
            try {
                writer.close();
                client.close();
            } catch (IOException e) {
                e.printStackTrace();
            }
        }
    }
}
