package com.example.inmemorydb.exceptions;

public class InvalidColumnException extends Throwable {
    public InvalidColumnException(String colName) {
        super(String.format("Invalid column %s", colName));
    }
}
