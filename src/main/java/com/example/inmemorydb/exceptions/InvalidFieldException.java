package com.example.inmemorydb.exceptions;

public class InvalidFieldException extends Exception {
    public InvalidFieldException(String fieldName) {
        super(String.format("Field %s not expected datatype", fieldName));
    }
}
