package com.example.inmemorydb.exceptions;

public class IndexWithNameAlreadyExistsException extends Throwable {
    public IndexWithNameAlreadyExistsException(String indexName, String tableName, String dbName) {
        super(String.format("Index with name %s already exists on %s.%s", indexName, tableName, dbName));
    }
}
