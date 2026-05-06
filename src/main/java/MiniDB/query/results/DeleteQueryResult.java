package MiniDB.query.results;

import MiniDB.query.QueryResult;

public class DeleteQueryResult implements QueryResult {
    private final String tableName;
    private final int deletedRows;

    public DeleteQueryResult(String tableName, int deletedRows) {
        this.tableName = tableName;
        this.deletedRows = deletedRows;
    }
    public String getTableName() {
        return tableName;
    }
    public int getDeletedRows() {
        return deletedRows;
    }
    public String getMessage() {
        return "Deleted " + deletedRows + " rows from " + tableName;
    }
}
