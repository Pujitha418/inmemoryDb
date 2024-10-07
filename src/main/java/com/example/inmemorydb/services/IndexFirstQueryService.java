package com.example.inmemorydb.services;

import com.example.inmemorydb.models.Row;
import org.springframework.stereotype.Service;

import java.util.List;
import java.util.Optional;

@Service
public class IndexFirstQueryService implements QueryHandler {
    private final QueryEngine queryEngine;

    public IndexFirstQueryService(QueryEngine queryEngine) {
        this.queryEngine = queryEngine;
    }

    public Optional<List<Row>> query(String dbName, String tableName, List<WhereCondition> whereConditions) {
        Optional<List<Row>> rows = queryEngine.queryByIndex(dbName, tableName, whereConditions);
        if (rows != null && rows.isPresent()) {
            return rows;
        }
        return queryEngine.queryByFullTableScan(dbName, tableName, whereConditions);
    }
}
