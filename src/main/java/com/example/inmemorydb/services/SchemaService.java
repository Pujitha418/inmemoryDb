package com.example.inmemorydb.services;

import com.example.inmemorydb.ColumnEntry;
import com.example.inmemorydb.exceptions.InvalidFieldException;
import com.example.inmemorydb.models.Column;
import com.example.inmemorydb.models.Schema;
import com.example.inmemorydb.store.DbStore;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;

import java.util.ArrayList;
import java.util.List;

@Service
public class SchemaService {
    private DbStore dbStore;

    @Autowired
    public SchemaService(DbStore dbStore) {
        this.dbStore = dbStore;
    }
    public List<ColumnEntry> createColumnEntry(String dbName, String tableName, List<Object> columns) throws InvalidFieldException {
        List<Column> schemaList = dbStore.getTableFromDb(dbName, tableName).getSchema().getColumnList();
        int schemaSize = schemaList.size();

        List<ColumnEntry> result = new ArrayList<>();

        for (int i = 0; i < schemaSize; i++) {
            Class<?> clazz = schemaList.get(i).getDataType();
            if (!clazz.isAssignableFrom(columns.get(i).getClass())) {
                throw new InvalidFieldException(schemaList.get(i).getName());
            }
            ColumnEntry<?> col = new ColumnEntry<>(columns.get(i));
            result.add(col);
        }

        return result;
    }
}
