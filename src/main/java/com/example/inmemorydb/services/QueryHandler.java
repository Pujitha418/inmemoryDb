package com.example.inmemorydb.services;

import com.example.inmemorydb.models.Row;
import org.springframework.stereotype.Component;

import java.util.List;
import java.util.Optional;

@Component
public interface QueryHandler {
    Optional<List<Row>> query(String dbName, String tableName, List<WhereCondition> whereConditions);
}
