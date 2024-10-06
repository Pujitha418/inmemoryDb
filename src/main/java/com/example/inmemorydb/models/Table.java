package com.example.inmemorydb.models;

import lombok.AllArgsConstructor;
import lombok.Getter;
import lombok.NoArgsConstructor;
import lombok.Setter;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

@Getter
@Setter
@AllArgsConstructor
@NoArgsConstructor
public class Table {
    private String name;
    private Schema schema;
    private List<Row> rowList;
    private List<Index> indexList;
    private Map<Index, Map<String, List<Row>>> indexTree;
}
