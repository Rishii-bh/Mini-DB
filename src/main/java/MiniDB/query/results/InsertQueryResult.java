package MiniDB.query.results;

import MiniDB.query.QueryResult;

public class InsertQueryResult implements QueryResult {
    private final int affectedRows;
    private final String message;

    public InsertQueryResult(int affectedRows, String message) {
        this.affectedRows = affectedRows;
        this.message = message;
    }

    public int getAffectedRows() {
        return affectedRows;
    }

    public String getMessage() {
        return message;
    }
}
