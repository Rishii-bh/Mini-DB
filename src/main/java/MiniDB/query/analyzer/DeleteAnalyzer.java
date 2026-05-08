package MiniDB.query.analyzer;

import MiniDB.StorageEngine.BinaryFileStorage;
import MiniDB.StorageEngine.PageFileStorage;
import MiniDB.core.Schema;
import MiniDB.query.condition.Condition;
import MiniDB.query.rawqueries.DeleteQuery;
import MiniDB.query.resolved.ResolvedDeleteQuery;

public class DeleteAnalyzer {
    private final PageFileStorage pageFileStorage;
    public DeleteAnalyzer(PageFileStorage pageFileStorage) {
        this.pageFileStorage = pageFileStorage;
    }

    public ResolvedDeleteQuery resolve(DeleteQuery query) {
        if(query.getTableName() == null || query.getTableName().isEmpty()) {
            throw new RuntimeException("Table name is null or empty");
        }
        String tableName = query.getTableName();
        if(!pageFileStorage.tableExists(tableName)) {
            throw new RuntimeException("Table " + tableName + " does not exist");
        }
        Schema originalSchema = pageFileStorage.getSchema(tableName);
        ConditionAnalyzer analyzer = new ConditionAnalyzer(originalSchema);
        Condition condition = analyzer.resolve(query.getRawCondition());
        condition.isValidValue(); //will throw error if literal dont match type
        condition.isValidOperator();

        //will throw error if col not found
        int colToCheckIndex = originalSchema.getColumnIndex(condition.getExpression1().getCol_name());
        return new ResolvedDeleteQuery(tableName, condition,colToCheckIndex);
    }
}
