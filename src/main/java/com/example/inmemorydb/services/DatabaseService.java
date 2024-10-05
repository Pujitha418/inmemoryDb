package com.example.inmemorydb.services;

import com.example.inmemorydb.exceptions.DbNotFoundException;
import com.example.inmemorydb.models.Database;
import com.example.inmemorydb.models.Table;
import com.example.inmemorydb.store.DbStore;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;

import java.util.ArrayList;
import java.util.List;
import java.util.Optional;

@Service
public class DatabaseService {
    private final DbStore dbStore;

    @Autowired
    public DatabaseService(DbStore dbStore) {
        this.dbStore = dbStore;
    }

    public Database createDatabase(String name) {
        Database database = new Database(name, null, 0);
        dbStore.addDbToStore(database);
        return database;
    }

    public void dropDatabase(Database database) {
        dbStore.deleteFromStore(database);
    }

    public List<Table> getTables(String dbName) throws DbNotFoundException {
        if (! dbStore.getDatabaseMap().containsKey(dbName)) {
            throw new DbNotFoundException(dbName);
        }
        List<Table> tables = new ArrayList<>();
        for (Database database: dbStore.getDatabaseMap().values()) {
            tables.addAll(database.getTableList());
        }
        return tables;
    }
}
