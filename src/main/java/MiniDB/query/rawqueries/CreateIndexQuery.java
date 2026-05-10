package MiniDB.query.rawqueries;

public class CreateIndexQuery {
    private final String tableName;
    private final String columnName;
    public CreateIndexQuery(String tableName, String columnName) {
        this.tableName = tableName;
        this.columnName = columnName;
    }

    public String getTableName() {
        return tableName;
    }
    public String getColumnName() {
        return columnName;
    }
}
