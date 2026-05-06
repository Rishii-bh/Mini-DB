package MiniDB.query.resolved;

import MiniDB.core.Row;
import MiniDB.core.Table;

public class ResolvedInsertQuery {
    private final String tableName;
    private final Row row;

    public ResolvedInsertQuery(String tableName, Row row) {
        this.tableName = tableName;
        this.row = row;
    }

    public String getTable() {
        return tableName;
    }
    public Row getRow() {
        return row;
    }
}
