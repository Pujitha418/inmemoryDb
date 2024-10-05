package com.example.inmemorydb.models;

import com.example.inmemorydb.ColumnEntry;
import lombok.AllArgsConstructor;
import lombok.Getter;
import lombok.NoArgsConstructor;
import lombok.Setter;

import java.util.List;

@Getter
@Setter
@AllArgsConstructor
@NoArgsConstructor
public class Row {
    List<ColumnEntry> columnEntries;
}
