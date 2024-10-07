package com.example.inmemorydb.store;

import com.example.inmemorydb.enums.IndexType;
import com.example.inmemorydb.exceptions.IndexWithNameAlreadyExistsException;
import com.example.inmemorydb.exceptions.InvalidColumnException;
import com.example.inmemorydb.models.*;
import lombok.Getter;
import lombok.Setter;
import org.javatuples.Pair;
import org.springframework.stereotype.Component;

import java.util.*;

@Getter
@Setter
@Component
public class DbStore {
    private Map<String, Database> databaseMap;
    private Map<Pair<String, String>, Table> tableMap;

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

        initializeTableDefaults(table);
        databaseMap.get(dbName).setTableList(new ArrayList<>());
        databaseMap.get(dbName).getTableList().add(table);
        tableMap.put(new Pair<>(dbName, table.getName()), table);
    }

    private static void initializeTableDefaults(Table table) {
        table.setRowList(new ArrayList<>());
        table.setIndexList(new ArrayList<>());
        table.setIndexTree(new HashMap<>());
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
        tableMap.get(tableKey).getRowList().add(row);
        return tableMap.get(tableKey);
    }

    public Table createIndexOnTable(String dbName, String tableName, String indexName,
                                    IndexType indexType, Set<String> columns) throws InvalidColumnException, IndexWithNameAlreadyExistsException {
        Index index = new Index(indexName, indexType, columns);

        //check if index name already exists on this table
        checkIfIndexAlreadyExists(dbName, tableName, indexName);
        
        //check for invalid column names
        validateIndexColumns(dbName, tableName, columns);

        //TODO: check if a index already exists on these columns
        tableMap.get(new Pair<>(dbName, tableName)).getIndexList().add(index);
        Table table = tableMap.get(new Pair<>(dbName, tableName));
        if (table.getRowList().isEmpty()) {
            return table;
        }
        Map<String, List<Row>> indexColumnValuesListMap = populateIndex(columns, table);
        tableMap.get(new Pair<>(dbName, tableName)).getIndexTree().put(index, indexColumnValuesListMap);

        return tableMap.get(new Pair<>(dbName, tableName));
    }

    private void checkIfIndexAlreadyExists(String dbName, String tableName, String newIndexName)
            throws IndexWithNameAlreadyExistsException {
        for (Index index : tableMap.get(new Pair<>(dbName, tableName)).getIndexList()) {
            if (index.getName().equals(newIndexName)) {
                throw new IndexWithNameAlreadyExistsException(newIndexName, tableName, dbName);
            }
        }
    }

    private Map<String, List<Row>> populateIndex(Set<String> columns, Table table) {
        Map<String, List<Row>> indexColumnValuesListMap = new HashMap<>();
        for (Row row: table.getRowList()) {
            StringBuilder key = new StringBuilder();
            for (int i = 0; i < row.getColumnEntries().size(); i++) {
                if (columns.contains(row.getColumnEntries().get(i).getColumn().getName())) {
                    key.append(row.getColumnEntries().get(i).getValue());
                    key.append('-');
                }
            }
            if (!key.isEmpty()) {
                key.deleteCharAt(key.length()-1);
            }
            if (indexColumnValuesListMap.containsKey(String.valueOf(key))) {
                indexColumnValuesListMap.get(String.valueOf(key)).add(row);
            } else {
                indexColumnValuesListMap.put(String.valueOf(key), new ArrayList<>(Arrays.asList(row)));
            }
        }
        return indexColumnValuesListMap;
    }

    private void validateIndexColumns(String dbName, String tableName, Set<String> indexColumns)
            throws InvalidColumnException {
        Set<String> columnSet = new HashSet<>();
        for (Column column: tableMap.get(new Pair<>(dbName, tableName)).getSchema().getColumnList()) {
            columnSet.add(column.getName());
        }
        for (String colName :  indexColumns) {
            if (! columnSet.contains(colName)) {
                throw new InvalidColumnException(colName);
            }
        }
    }

    public Pair<Index, Map<String, List<Row>>> getIndexForColumns(String dbName, String tableName, List<String> indexColumns) {
        for (Index index : tableMap.get(new Pair<>(dbName, tableName)).getIndexList()) {
            var cnt = 0;
            for (String colName: index.getColumns()) {
                if (indexColumns.contains(colName)) {
                    cnt += 1;
                }
            }
            if (cnt == index.getColumns().size()) {
                return new Pair<>(index, tableMap.get(new Pair<>(dbName, tableName)).getIndexTree().get(index));
            }
        }
        Pair<Index, Map<String, List<Row>>> left;
        Pair<Index, Map<String, List<Row>>> right = null;
        if (!indexColumns.isEmpty()) {
            left = getIndexForColumns(dbName, tableName, indexColumns.subList(0, indexColumns.size()-1));
            if (left == null) {
                right = getIndexForColumns(dbName, tableName, indexColumns.subList(1, indexColumns.size()));
            } else {
                return left;
            }
        }
        return right;
    }

    public List<Row> getAllTableRows(String dbName, String tableName) {
        return tableMap.get(new Pair<>(dbName, tableName)).getRowList();
    }
}
