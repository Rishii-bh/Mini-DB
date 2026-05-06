package MiniDB.query.results;

import MiniDB.query.QueryResult;

public class CreateTableQueryResult implements QueryResult {
    private final String tableName;
    private final int columnCount;

    public CreateTableQueryResult(String tableName, int columnCount) {
        this.tableName = tableName;
        this.columnCount = columnCount;
    }
    public String getTableName() {
        return tableName;
    }
    public int getColumnCount() {
        return columnCount;
    }

    public String getMessage(){
        return "Table " + tableName + " has " + columnCount + " columns.";
    }
}
