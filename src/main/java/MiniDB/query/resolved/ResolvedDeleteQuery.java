package MiniDB.query.resolved;

import MiniDB.query.condition.Condition;

public class ResolvedDeleteQuery {
    private final String tableName;
    private final Condition condition;
    private final int colToCheckIndex;

    public ResolvedDeleteQuery(String tableName, Condition condition, int colToCheckIndex) {
        this.tableName = tableName;
        this.condition = condition;
        this.colToCheckIndex = colToCheckIndex;
    }

    public String getTableName() {
        return tableName;
    }

    public int getColToCheckIndex() {
        return colToCheckIndex;
    }
    public Condition getCondition() {
        return condition;
    }
}
