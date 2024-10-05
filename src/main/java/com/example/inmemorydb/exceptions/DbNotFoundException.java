package com.example.inmemorydb.exceptions;

public class DbNotFoundException extends Exception {
    public DbNotFoundException(String dbName) {
        super(String.format("db with name %s not found", dbName));
    }
}
