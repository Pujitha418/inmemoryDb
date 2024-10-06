package com.example.inmemorydb;

import com.example.inmemorydb.exceptions.IndexWithNameAlreadyExistsException;
import com.example.inmemorydb.exceptions.InvalidColumnException;
import com.example.inmemorydb.exceptions.InvalidFieldException;
import com.example.inmemorydb.models.*;
import com.example.inmemorydb.services.DatabaseService;
import com.example.inmemorydb.services.SchemaService;
import com.example.inmemorydb.services.TableService;
import lombok.extern.log4j.Log4j2;
import org.springframework.context.annotation.DependsOn;
import org.springframework.core.annotation.Order;
import org.springframework.stereotype.Component;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Set;

@Component
@Log4j2
//@DependsOn({DatabaseService.class, TableService.class,  SchemaService.class})
public class Runner {
    private DatabaseService databaseService;
    private TableService tableService;

    public Runner(DatabaseService databaseService, TableService tableService) {
        this.databaseService = databaseService;
        this.tableService = tableService;
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
            tableService.createIndex(db.getName(), table.getName(), "index_col2", Set.of("col2"));

        } catch (InvalidFieldException | InvalidColumnException | IndexWithNameAlreadyExistsException e) {
            log.error(e.getMessage());
        }

    }
}
