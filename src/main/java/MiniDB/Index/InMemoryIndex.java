package MiniDB.Index;

import MiniDB.StorageEngine.RecordId;
import MiniDB.core.Value;

import java.util.*;

public class InMemoryIndex {
    private final String tableName;
    private final String columnName;
    protected final Map<Value, LinkedHashSet<RecordId>> indexEntries;

    public InMemoryIndex(String tableName, String columnName , Map<Value, LinkedHashSet<RecordId>> indexEntries) {
        this.tableName = tableName;
        this.columnName = columnName;
        this.indexEntries = indexEntries;
    }
    public void add(Value key, RecordId recordId) {
        indexEntries.computeIfAbsent(key, k -> new LinkedHashSet<>()).add(recordId);
    }
    public LinkedHashSet<RecordId> get(Value key) {
        return indexEntries.getOrDefault(key, new LinkedHashSet<>());
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
