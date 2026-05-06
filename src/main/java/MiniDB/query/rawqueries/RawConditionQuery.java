package MiniDB.query.rawqueries;

import MiniDB.query.condition.Operator;

public class RawConditionQuery {
    private final String colName;
    private final Operator operator;
    private final Object value;

    public RawConditionQuery(String colName, Operator operator, Object value) {
        this.colName = colName;
        this.operator = operator;
        this.value = value;
    }

    public String getColName() {
        return colName;
    }
    public Operator getOperator() {
        return operator;
    }
    public Object getValue() {
        return value;
    }
}
