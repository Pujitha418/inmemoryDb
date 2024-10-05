package com.example.inmemorydb;

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
            columnList.add(new Column("column1", Integer.class));
            schema.setColumnList(columnList);
            Table table = tableService.createTable(db.getName(), "table1", schema);
            log.info("table - {}", table);

            List<Object> fieldValues = List.of("klk");
            Table table1 = tableService.addRowToTable(db.getName(), table.getName(), fieldValues);
            log.info("table after inserting row - {}", table1);
            for (Row row: table1.getRowList()) {
                log.info("row.getColumnEntries() = " + row.getColumnEntries().get(0).getValue());
            }
        } catch (InvalidFieldException e) {
            log.error(e.getMessage());
        }

    }
}
