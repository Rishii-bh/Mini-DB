package MiniDB.query.analyzer;

import MiniDB.core.Column;
import MiniDB.core.Schema;
import MiniDB.query.condition.Condition;
import MiniDB.query.rawqueries.RawConditionQuery;

public class ConditionAnalyzer {
    private final Schema schema;
    public ConditionAnalyzer( Schema schema) {
        this.schema = schema;
    }

    public Condition resolve(RawConditionQuery query) {
        //will throw error if colName dont exist in schema;
       int colIndex = schema.getColumnIndex(query.getColName());
       Column column = schema.getColumn(colIndex);
       return new Condition(column, query.getOperator(), query.getValue());
    }
}
