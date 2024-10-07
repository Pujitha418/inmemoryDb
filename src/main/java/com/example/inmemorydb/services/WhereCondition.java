package com.example.inmemorydb.services;

import lombok.AllArgsConstructor;
import lombok.Getter;
import lombok.NoArgsConstructor;
import lombok.Setter;

@Getter
@Setter
@AllArgsConstructor
@NoArgsConstructor
public class WhereCondition {
    private String columnName;
    private Object columnValue;
    //Map<String, ?> conditions;
}
