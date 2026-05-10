package MiniDB.query.resolved;

public class ResolvedCreateIndexQuery {
    private final String tableName;
    private final String columnName;
    public ResolvedCreateIndexQuery(String tableName, String columnName) {
        this.tableName = tableName;
        this.columnName = columnName;
    }

    public String getColumnName() {
        return columnName;
    }
    public String getTableName() {
        return tableName;
    }
}
