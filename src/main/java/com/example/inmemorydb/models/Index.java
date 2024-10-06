package com.example.inmemorydb.models;

import com.example.inmemorydb.enums.IndexType;
import lombok.AllArgsConstructor;
import lombok.Getter;
import lombok.NoArgsConstructor;
import lombok.Setter;

import java.util.List;
import java.util.Set;

@Getter
@Setter
@AllArgsConstructor
@NoArgsConstructor
public class Index {
    private String name;
    private IndexType indexType;
    private Set<String> columns;
}
