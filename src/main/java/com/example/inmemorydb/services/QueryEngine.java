package com.example.inmemorydb.services;

import com.example.inmemorydb.models.Column;
import com.example.inmemorydb.models.ColumnEntry;
import com.example.inmemorydb.models.Index;
import com.example.inmemorydb.models.Row;
import com.example.inmemorydb.store.DbStore;
import org.javatuples.Pair;
import org.springframework.stereotype.Component;

import java.util.*;

@Component
public class QueryEngine {
    private final DbStore dbStore;

    public QueryEngine(DbStore dbStore) {
        this.dbStore = dbStore;
    }

    public Optional<List<Row>> queryByIndex(String dbName, String tableName, List<WhereCondition> conditions) {
        List<String> columnList = new ArrayList<>();
        for (Column column: dbStore.getTableMap().get(new Pair<>(dbName, tableName)).getSchema().getColumnList()) {
            for (WhereCondition condition: conditions) {
                if (Objects.equals(condition.getColumnName(), column.getName())) {
                    columnList.add(column.getName());
                }
             }
        }
        Pair<Index, Map<String, List<Row>>> indexMapMap = dbStore.getIndexForColumns(dbName, tableName,columnList);
        if (indexMapMap == null) {
            //no index found on querying columns. Hence returning null.
            return Optional.empty();
        }

        Map<String, Object> columnValues = convertWhereConditionsToMap(conditions);
        StringBuilder key = getKey(indexMapMap, columnValues);
        List<Row> rows = filterByIndex(indexMapMap, key);
        Set<Integer> indexesToBeRemoved = filterByRemainingFields(rows, columnValues);
        List<Row> result = new ArrayList<>();
        for (int i = 0; i < rows.size(); i++) {
            if (! indexesToBeRemoved.contains(i)) {
                result.add(rows.get(i));
            }
        }
        return Optional.of(result);
    }

    private static List<Row> filterByIndex(Pair<Index, Map<String, List<Row>>> indexMapMap, StringBuilder key) {
        List<Row> rows = new ArrayList<>();
        for (Map.Entry<String, List<Row>> rowList : indexMapMap.getValue1().entrySet()) {
            if (rowList.getKey().equals(String.valueOf(key))) {
                rows.addAll(rowList.getValue());
            }
        }
        return rows;
    }

    private static Set<Integer> filterByRemainingFields(List<Row> rows, Map<String, Object> columnValues) {
        Set<Integer> indexesToBeRemoved = new HashSet<>();
        for (int i = 0; i < rows.size(); i++) {
            Row row = rows.get(i);
            for (ColumnEntry columnEntry: row.getColumnEntries()) {
                if (columnValues.containsKey(columnEntry.getColumn().getName())
                        && ! columnValues.get(columnEntry.getColumn().getName()).equals(columnEntry.getValue())) {
                    indexesToBeRemoved.add(i);
                }
            }
        }
        return indexesToBeRemoved;
    }

    private static Map<String, Object> convertWhereConditionsToMap(List<WhereCondition> conditions) {
        Map<String, Object> columnValues = new HashMap<>();
        for (WhereCondition condition: conditions) {
            columnValues.put(condition.getColumnName(), condition.getColumnValue());
        }
        return columnValues;
    }

    private static StringBuilder getKey(Pair<Index, Map<String, List<Row>>> indexMapMap, Map<String, Object> columnValues) {
        StringBuilder key = new StringBuilder();
        for (String column: indexMapMap.getValue0().getColumns()) {
            key.append(columnValues.get(column));
            key.append("-");
            columnValues.remove(column);
        }

        if (! key.isEmpty()) {
            key.deleteCharAt(key.length()-1);
        }
        return key;
    }

    public Optional<List<Row>> queryByFullTableScan(String dbName, String tableName, List<WhereCondition> conditions) {
        List<Row> rows = dbStore.getAllTableRows(dbName, tableName);
        Map<String, Object> columnValues = convertWhereConditionsToMap(conditions);

        List<Row> result = new ArrayList<>();
        for (Row row: rows) {
            boolean addToResult = true;
            for (ColumnEntry columnEntry: row.getColumnEntries()) {
                if (columnValues.containsKey(columnEntry.getColumn().getName())
                        && ! columnValues.get(columnEntry.getColumn().getName()).equals(columnEntry.getValue())) {
                    addToResult = false;
                    break;
                }
            }
            if (addToResult) {
                result.add(row);
            }
        }

        return Optional.of(result);
    }
}
