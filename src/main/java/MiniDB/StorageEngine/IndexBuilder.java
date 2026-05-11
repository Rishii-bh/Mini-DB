package MiniDB.StorageEngine;

import MiniDB.core.Row;
import MiniDB.core.Schema;
import MiniDB.core.Type;
import MiniDB.core.Value;

import java.util.*;

public class IndexBuilder {
    PageFileStorage storage;
    public IndexBuilder(PageFileStorage storage) {
       this.storage = storage;
    }

    public InMemoryIndex build(String tableName, String columnName) {
        if(!storage.tableExists(tableName)) {
            throw new IndexException("Table " + tableName + " does not exist");
        }
        Schema schema = storage.getSchema(tableName);
        int colIndex = schema.getColumnIndex(columnName);
        List<RowWithRecordId> rowsWithRecordIds = storage.scanRows(tableName);
        Map<Value , LinkedHashSet<RecordId>> index = new HashMap<>();
        for(RowWithRecordId rowWithRecordId : rowsWithRecordIds) {
            Row row = rowWithRecordId.row();
            Value value = extract(row,schema,colIndex);
          index.computeIfAbsent(value, k -> new LinkedHashSet<>()).add(rowWithRecordId.recordId());
        }
        return new InMemoryIndex(tableName,columnName,index);
    }

    protected Value extract(Row row , Schema schema, int columnIndex) {
        Object value = row.getValue(columnIndex);
        Type type = schema.getColumn(columnIndex).getType();
        if(checkType(type,value)) {
            return new Value(type, value);
        }
        else {
            throw new IndexException("Column " + columnIndex + " is not of type " + type);
        }

    }

    private boolean checkType(Type type ,Object value) {
        return switch (type) {
            case INT -> value instanceof Integer;
            case TEXT -> value instanceof String;
            case BOOL -> value instanceof Boolean;
            default -> throw new IndexException("Type " + type + " not supported");
        };
    }


}
