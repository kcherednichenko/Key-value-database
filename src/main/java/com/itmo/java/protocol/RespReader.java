package com.itmo.java.protocol;

import com.itmo.java.protocol.model.*;

import java.io.*;
import java.nio.ByteBuffer;
import java.util.LinkedList;
import java.util.List;

public class RespReader implements AutoCloseable {
    private final BufferedReader bufferedReader;

    /**
     * Специальные символы окончания элемента
     */
    private static final byte CR = '\r';
    private static final byte LF = '\n';

    public RespReader(InputStream is) {
        bufferedReader = new BufferedReader(new InputStreamReader(is));
    }

    /**
     * Есть ли следующий массив в стриме?
     */
    public boolean hasArray() throws IOException {
        byte read = (byte) bufferedReader.read();
        return read == RespArray.CODE;
    }

    /**
     * Считывает из input stream следующий объект. Может прочитать любой объект, сам определит его тип на основе кода объекта.
     * Например, если первый элемент "-", то вернет ошибку. Если "$" - bulk строку
     *
     * @throws EOFException если stream пустой
     * @throws IOException  при ошибке чтения
     */
    public RespObject readObject() throws IOException {
        int read = bufferedReader.read();
        if (read == -1) {
            throw new EOFException("Stream is empty");
        }
        byte readBytes = (byte) read;
        if (readBytes == RespArray.CODE) {
            return readArray();
        } else if (readBytes == RespBulkString.CODE) {
            return readBulkString();
        } else if (readBytes == RespCommandId.CODE) {
            return readCommandId();
        } else if (readBytes == RespError.CODE) {
            return readError();
        } else {
            throw new IOException("Error while reading");
        }
    }

    /**
     * Считывает объект ошибки
     *
     * @throws EOFException если stream пустой
     * @throws IOException  при ошибке чтения
     */
    public RespError readError() throws IOException {
        return new RespError(myRead());
    }

    /**
     * Читает bulk строку
     *
     * @throws EOFException если stream пустой
     * @throws IOException  при ошибке чтения
     */
    public RespBulkString readBulkString() throws IOException {
        byte[] bytes = myRead();
        int size = Integer.parseInt(new String(bytes));
        if (size == RespBulkString.NULL_STRING_SIZE) {
            return RespBulkString.NULL_STRING;
        }
        byte[] stringData = myRead();
        if (stringData.length != size) {
            throw new IOException("Error while reading");
        }
        return new RespBulkString(stringData);
    }

    /**
     * Считывает массив RESP элементов
     *
     * @throws EOFException если stream пустой
     * @throws IOException  при ошибке чтения
     */
    public RespArray readArray() throws IOException {
        byte[] arraySizeBytes = myRead();
        int arraySize = Integer.parseInt(new String(arraySizeBytes));
        RespObject[] respObjectArray = new RespObject[arraySize];
        for (int i = 0; i < arraySize; i++) {
            respObjectArray[i] = readObject();
        }
        return new RespArray(respObjectArray);
    }

    /**
     * Считывает id команды
     *
     * @throws EOFException если stream пустой
     * @throws IOException  при ошибке чтения
     */
    public RespCommandId readCommandId() throws IOException {
        byte[] data = myRead();

        return new RespCommandId(ByteBuffer.wrap(data).getInt());
    }


    @Override
    public void close() throws IOException {
        bufferedReader.close();
    }

    private byte[] myRead() throws IOException {
        List<Byte> messageBytes = new LinkedList<>();

        byte readByte = (byte) bufferedReader.read();
        if (readByte == -1) {
            throw new EOFException("Stream is empty");
        }

        messageBytes.add(readByte);
        byte firstByte = 0;
        byte secondByte = 0;

        while (firstByte != CR || secondByte != LF) {
            firstByte = secondByte;
            secondByte = (byte) bufferedReader.read();
            if (secondByte == -1) {
                throw  new IOException("Error while reading");
            }
            messageBytes.add(secondByte);
        }
        byte[] message = new byte[messageBytes.size() - 2];
        for (int i = 0; i < messageBytes.size() - 2; i++) {
            message[i] = messageBytes.get(i);
        }

        return message;
    }
}