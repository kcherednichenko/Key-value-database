package com.itmo.java.client.connection;

import com.itmo.java.client.exception.ConnectionException;
import com.itmo.java.protocol.RespReader;
import com.itmo.java.protocol.RespWriter;
import com.itmo.java.protocol.model.RespArray;
import com.itmo.java.protocol.model.RespObject;

import java.io.*;
import java.net.Socket;

/**
 * С помощью {@link RespWriter} и {@link RespReader} читает/пишет в сокет
 */
public class SocketKvsConnection implements KvsConnection {
    private final ConnectionConfig config;
    private final Socket clientSocket;
    private final RespWriter writer;
    private final RespReader reader;

    public SocketKvsConnection(ConnectionConfig config) {
        this.config = config;
        try {
            this.clientSocket = new Socket(config.getHost(), config.getPort());
            this.reader = new RespReader(clientSocket.getInputStream());
            this.writer = new RespWriter(clientSocket.getOutputStream());
        } catch (IOException e) {
            throw new RuntimeException("Can't create client socket");
        }
    }

    /**
     * Отправляет с помощью сокета команду и получает результат.
     *
     * @param commandId id команды (номер)
     * @param command   команда
     * @throws ConnectionException если сокет закрыт или если произошла другая ошибка соединения
     */
    @Override
    public synchronized RespObject send(int commandId, RespArray command) throws ConnectionException {
        try {
            RespWriter respWriter = new RespWriter(clientSocket.getOutputStream());
            respWriter.write(command);
            RespReader respReader = new RespReader(clientSocket.getInputStream());
            return respReader.readObject();
        } catch (IOException e) {
            close();
            throw new ConnectionException("Error in input/output stream", e);
        }
    }

    /**
     * Закрывает сокет (и другие использованные ресурсы)
     */
    @Override
    public void close() {
        try {
            writer.close();
            reader.close();
            clientSocket.close();
        } catch (IOException e) {
            e.printStackTrace();
        }
    }
}
