package MiniDB.query.resolved;

import MiniDB.core.Schema;
import MiniDB.core.Table;
import MiniDB.query.condition.Condition;

import java.util.ArrayList;
import java.util.List;
import java.util.Optional;

public class ResolvedSelectQuery {
    private final String tableName;
    private final List<Integer> selectedColIndex;
    private final Schema resultSchema;
    private final Condition resolvedCondition;

    public ResolvedSelectQuery(String tableName,List<Integer> selectedColIndex, Schema resultSchema, Condition resolvedCondition) {
        this.tableName = tableName;
        this.selectedColIndex = new ArrayList<>(selectedColIndex);
        this.resultSchema = resultSchema;
        this.resolvedCondition = resolvedCondition;
    }
    public ResolvedSelectQuery(String tableName,List<Integer> selectedColIndex, Schema resultSchema) {
        this(tableName,selectedColIndex,resultSchema,null);
    }

    public Schema getResultSchema() {
        return resultSchema;
    }
    public Optional<Condition> getResolvedCondition() {
        return Optional.ofNullable(resolvedCondition);
    }
    public List<Integer> getSelectedColIndex() {
        return selectedColIndex;
    }
    public String getTableName() {
        return tableName;
    }

}
