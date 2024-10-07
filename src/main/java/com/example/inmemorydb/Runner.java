package com.example.inmemorydb;

import com.example.inmemorydb.exceptions.IndexWithNameAlreadyExistsException;
import com.example.inmemorydb.exceptions.InvalidColumnException;
import com.example.inmemorydb.exceptions.InvalidFieldException;
import com.example.inmemorydb.models.*;
import com.example.inmemorydb.services.*;
import lombok.extern.log4j.Log4j2;
import org.springframework.stereotype.Component;

import java.util.*;

@Component
@Log4j2
//@DependsOn({DatabaseService.class, TableService.class,  SchemaService.class})
public class Runner {
    private DatabaseService databaseService;
    private TableService tableService;
    private IndexFirstQueryService indexFirstQueryService;

    public Runner(DatabaseService databaseService, TableService tableService, IndexFirstQueryService indexFirstQueryService) {
        this.databaseService = databaseService;
        this.tableService = tableService;
        this.indexFirstQueryService = indexFirstQueryService;
        run();
    }

    public void run() {
        try {
            Database db = databaseService.createDatabase("testdb");
            Schema schema = new Schema();
            List<Column> columnList = new ArrayList<>();
            columnList.add(new Column("col1", Integer.class));
            columnList.add(new Column("col2", String.class));
            schema.setColumnList(columnList);
            Table table = tableService.createTable(db.getName(), "table1", schema);
            log.info("table - {}", table);

            List<Object> fieldValues = List.of(1, "text1");
            tableService.addRowToTable(db.getName(), table.getName(), fieldValues);
            fieldValues = List.of(2, "text2");
            tableService.addRowToTable(db.getName(), table.getName(), fieldValues);
            fieldValues = List.of(3, "text1");
            Table table1 = tableService.addRowToTable(db.getName(), table.getName(), fieldValues);
            log.info("table after inserting row - {}", table1);
            for (Row row: table1.getRowList()) {
                log.info("row.getColumnEntries() = " + row.getColumnEntries().get(0).getValue());
            }

            Table indexedTable = tableService.createIndex(db.getName(), table.getName(), "index_col2", Set.of("col2"));
            for (Map.Entry<Index, Map<String, List<Row>>> entry: indexedTable.getIndexTree().entrySet()) {
                System.out.println("entry.getKey() = " + entry.getKey());
                System.out.println("entry.getValue().keySet() = " + entry.getValue().keySet());
                System.out.println("entry.getValue().values() = " + entry.getValue().values());
            }

            //checking if duplicate index name check is working
            //tableService.createIndex(db.getName(), table.getName(), "index_col2", Set.of("col2"));
            System.out.println("<<<<<<<<Querying>>>>>>>>>>>>");
            List<WhereCondition> conditions = new ArrayList<>();
            conditions.add(new WhereCondition("col1", 3));
            Optional<List<Row>> resultSet = indexFirstQueryService.query(db.getName(), table.getName(), conditions);
            if (resultSet.isEmpty()) {
                log.warn("No results for given criteria");
            } else {
                for (Row row: resultSet.get()) {
                    for (ColumnEntry columnEntry: row.getColumnEntries()) {
                        System.out.println("columnEntry.getColumn() = " + columnEntry.getColumn());
                        System.out.println("columnEntry.getValue() = " + columnEntry.getValue());
                    }
                 }
            }

            System.out.println("<<<<<<<<Querying by indexed column>>>>>>>>>>>>");
            List<WhereCondition> conditions1 = new ArrayList<>();
            conditions1.add(new WhereCondition("col2", "text1"));
            Optional<List<Row>> rs1 = indexFirstQueryService.query(db.getName(), table.getName(), conditions1);
            if (rs1.isEmpty()) {
                log.warn("No results for given criteria");
            } else {
                log.info("Result set size - {}", rs1.get().size());
                for (Row row: rs1.get()) {
                    for (ColumnEntry columnEntry: row.getColumnEntries()) {
                        System.out.println("columnEntry.getColumn() = " + columnEntry.getColumn().getName());
                        //System.out.println("columnEntry.getColumn() = " + columnEntry.getColumn());
                        System.out.println("columnEntry.getValue() = " + columnEntry.getValue());
                    }
                }
            }


        } catch (InvalidFieldException | InvalidColumnException | IndexWithNameAlreadyExistsException e) {
            log.error(e.getMessage());
        }

    }
}
