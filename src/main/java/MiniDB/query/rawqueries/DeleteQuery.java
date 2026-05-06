package MiniDB.query.rawqueries;

import MiniDB.query.Query;

public class DeleteQuery implements Query {
    private final String tableName;
    private final RawConditionQuery condition;

    public DeleteQuery(String tableName, RawConditionQuery condition) {
        this.tableName = tableName;
        this.condition = condition;
    }

    public String getTableName(){
        return tableName;
    }
    public RawConditionQuery getRawCondition(){
        return condition;
    }
}
