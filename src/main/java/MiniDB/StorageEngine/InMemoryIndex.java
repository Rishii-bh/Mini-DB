package MiniDB.StorageEngine;

import MiniDB.core.Value;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class InMemoryIndex {
    private final String tableName;
    private final String columnName;
    private final Map<Value, List<RecordId>> indexEntries;

    public InMemoryIndex(String tableName, String columnName , Map<Value, List<RecordId>> indexEntries) {
        this.tableName = tableName;
        this.columnName = columnName;
        this.indexEntries = indexEntries;
    }
    public void add(Value key, RecordId recordId) {
        indexEntries.computeIfAbsent(key, k -> new ArrayList<>()).add(recordId);
    }
    public List<RecordId> get(Value key) {
        return indexEntries.getOrDefault(key, new ArrayList<>());
    }
    public String getTableName() {
        return tableName;
    }
    public String getColumnName() {
        return columnName;
    }
    public void delete(Value key) {
        indexEntries.remove(key);
    }
}
