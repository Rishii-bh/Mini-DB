package MiniDB.query.results;

import MiniDB.core.Column;
import MiniDB.core.Row;
import MiniDB.core.Schema;
import MiniDB.query.QueryResult;

import java.util.List;

public class SelectQueryResult implements QueryResult {
    private final Schema resultSchema;
    private final List<Row> resultRows;

    public SelectQueryResult(Schema resultSchema, List<Row> resultRows) {
        this.resultSchema = resultSchema;
        this.resultRows = resultRows;
    }

    public int getRowCount() {
        return resultRows.size();
    }
    public Row getRow(int index) {
        return resultRows.get(index);
    }
    public Schema getResultSchema() {
        return resultSchema;
    }
    public List<Row> getResultRows() {
        return resultRows;
    }
}
