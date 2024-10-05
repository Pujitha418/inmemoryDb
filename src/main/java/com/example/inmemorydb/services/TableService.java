package com.example.inmemorydb.services;

import com.example.inmemorydb.exceptions.InvalidFieldException;
import com.example.inmemorydb.models.Row;
import com.example.inmemorydb.models.Schema;
import com.example.inmemorydb.models.Table;
import com.example.inmemorydb.store.DbStore;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;

import java.util.List;

@Service
public class TableService {
    private final DbStore dbStore;
    private final SchemaService schemaService;

    @Autowired
    public TableService(DbStore dbStore, SchemaService schemaService) {
        this.dbStore = dbStore;
        this.schemaService = schemaService;
    }

    public Table createTable(String dbName, String tableName, Schema schema) {
        Table table = new Table(tableName, schema, null);
        dbStore.addTableToDb(dbName, table);
        return table;
    }

    public Table addRowToTable(String dbName, String tableName, List<Object> fieldValues) throws InvalidFieldException {
        Row row = new Row();
        row.setColumnEntries(schemaService.createColumnEntry(dbName, tableName, fieldValues));
        return dbStore.addRowToTable(dbName, tableName, row);
    }
}
