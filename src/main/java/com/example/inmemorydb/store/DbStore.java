package com.example.inmemorydb.store;

import com.example.inmemorydb.models.Database;
import com.example.inmemorydb.models.Row;
import com.example.inmemorydb.models.Table;
import lombok.AllArgsConstructor;
import lombok.Getter;
import lombok.NoArgsConstructor;
import lombok.Setter;
import org.javatuples.Pair;
import org.springframework.stereotype.Component;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

@Getter
@Setter
@Component
public class DbStore {
    Map<String, Database> databaseMap;
    Map<Pair<String, String>, Table> tableMap;

    public DbStore() {
        this.databaseMap = new HashMap<>();
        this.tableMap = new HashMap<>();
    }

    public void addDbToStore(Database database) {
        databaseMap.put(database.getName(), database);
    }

    public void deleteFromStore(Database database) {
        databaseMap.remove(database.getName());
    }

    public void addTableToDb(String dbName, Table table) {
        if (!databaseMap.containsKey(dbName)) {
            return;
        }
        databaseMap.get(dbName).setTableList(new ArrayList<>());
        databaseMap.get(dbName).getTableList().add(table);
        tableMap.put(new Pair<>(dbName, table.getName()), table);
    }

    public Table getTableFromDb(String dbName, String tableName) {
        if (!databaseMap.containsKey(dbName)) {
            return null;
        }
        return tableMap.get(new Pair<>(dbName, tableName));
    }

    public Table addRowToTable(String dbName, String tableName, Row row) {
        Pair<String, String> tableKey = new Pair<>(dbName, tableName);
        if (!databaseMap.containsKey(dbName) || !tableMap.containsKey(tableKey)) {
            return null;
        }
        if (tableMap.get(tableKey).getRowList() == null) {
            tableMap.get(tableKey).setRowList(new ArrayList<>());
        }
        tableMap.get(tableKey).getRowList().add(row);
        return tableMap.get(tableKey);
    }
}
