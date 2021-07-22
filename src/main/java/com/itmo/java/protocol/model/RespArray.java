package com.itmo.java.protocol.model;

import java.io.IOException;
import java.io.OutputStream;
import java.nio.ByteBuffer;
import java.util.*;
import java.util.stream.Collectors;

/**
 * Массив RESP объектов
 */
public class RespArray implements RespObject {

    /**
     * Код объекта
     */

    private final RespObject[] objects;

    public static final byte CODE = '*';

    public RespArray(RespObject... objects) {
        this.objects = objects;
    }

    /**
     * Ошибка ли это? Ответ - нет
     *
     * @return false
     */
    @Override
    public boolean isError() {
        return false;
    }

    /**
     * Строковое представление
     *
     * @return результаты метода {@link RespObject#asString()} для всех хранимых объектов, разделенные пробелом
     */
    @Override
    public String asString() {
        StringJoiner resultStringJoiner = new StringJoiner(" ");
        for (RespObject respObject : objects) {
           resultStringJoiner.add(respObject.asString());
       }
        return resultStringJoiner.toString();
    }

    @Override
    public void write(OutputStream os) throws IOException {
        os.write(CODE);
        os.write(String.valueOf(objects.length).getBytes());
        os.write(CRLF);

        for (RespObject ro : objects) {
            ro.write(os);
        }
    }

    public List<RespObject> getObjects() {
        return List.of(objects);
    }
}
