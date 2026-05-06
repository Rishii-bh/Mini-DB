package MiniDB.core;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class Schema {
    public List<Column> columns;
    public Map<String , Integer> columnIndex;

    public Schema(List<Column> columns) {
        this.columns = new ArrayList<>(columns);
        this.columnIndex = new HashMap<String , Integer>();

        for(int i = 0; i < columns.size(); i++) {
            String colName = columns.get(i).getCol_name();
            if(columnIndex.containsKey(colName)) {
                throw new IllegalArgumentException("Duplicate column name: " + colName);
            }
            columnIndex.put(colName, i);
        }
    }

    public Column getColumn(int index) {
        return columns.get(index);
    }

    public int size() {
        return this.columns.size();
    }

    public Integer getColumnIndex(String colName) {
        Integer colIndex = columnIndex.get(colName);
        if(colIndex == null) {
            throw new IllegalArgumentException("Column not found: " + colName);
        }
        return colIndex;
    }

    public List<Column> getColumns() {
        return this.columns;
    }

}
