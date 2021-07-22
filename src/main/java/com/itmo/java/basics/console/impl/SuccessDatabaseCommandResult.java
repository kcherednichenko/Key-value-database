package com.itmo.java.basics.console.impl;

import com.itmo.java.basics.console.DatabaseCommandResult;
import com.itmo.java.protocol.model.RespBulkString;
import com.itmo.java.protocol.model.RespError;
import com.itmo.java.protocol.model.RespObject;

/**
 * Результат успешной команды
 */
public class SuccessDatabaseCommandResult implements DatabaseCommandResult {
    private final byte[] payLoad;

    public SuccessDatabaseCommandResult(byte[] payload) {
        this.payLoad = payload;
    }

    @Override
    public String getPayLoad() {
        if (payLoad == null) {
            return null;
        }
        return new String(payLoad);
    }

    @Override
    public boolean isSuccess() {
        return true;
    }

    /**
     * Сериализуется в {@link RespBulkString}
     */
    @Override
    public RespObject serialize() {
        RespBulkString errorRespBulkString = new RespBulkString(payLoad);
        return errorRespBulkString;
    }
}
