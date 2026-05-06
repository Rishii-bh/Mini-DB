package MiniDB.query.resolved;

import MiniDB.core.Column;

import java.util.List;

public class ResolvedCreateTableQuery {
    private final String tableName;
    private final List<Column> columns;

    public ResolvedCreateTableQuery(String tableName, List<Column> columns) {
        this.tableName = tableName;
        this.columns = columns;
    }
    public String getTableName() {
        return tableName;
    }
    public List<Column> getColumns() {
        return columns;
    }
}
