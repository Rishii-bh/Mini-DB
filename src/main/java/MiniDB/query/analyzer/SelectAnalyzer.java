package MiniDB.query.analyzer;

import MiniDB.StorageEngine.BinaryFileStorage;
import MiniDB.StorageEngine.PageFileStorage;
import MiniDB.StorageEngine.TextFileStorageEngine;
import MiniDB.core.Column;
import MiniDB.core.Schema;
import MiniDB.query.condition.Condition;
import MiniDB.query.rawqueries.RawConditionQuery;
import MiniDB.query.rawqueries.SelectQuery;
import MiniDB.query.resolved.ResolvedSelectQuery;

import java.util.ArrayList;
import java.util.List;
import java.util.Optional;

public class SelectAnalyzer {
    private final PageFileStorage pageFileStorage;

    public SelectAnalyzer(PageFileStorage binaryFileStorage) {
        this.pageFileStorage = binaryFileStorage;

    }

    public ResolvedSelectQuery resolve(SelectQuery query) {
        String tableName = query.getTableName();
        if(tableName == null||tableName.isEmpty()) {
            throw new RuntimeException("Table name is null or empty");
        }
        if(!pageFileStorage.tableExists(tableName)) {
            throw new RuntimeException("Table " + query.getTableName() + " does not exist");
        }
        Schema tableSchema = pageFileStorage.getSchema(tableName);
        List<Column> resultColumns = new ArrayList<Column>();

        List<Integer> selectedColIndex = new ArrayList<>();
        for(int i=0; i<query.getColNames().size(); i++) {
            //returns the index of the column which corresponds to the query col
            int index = tableSchema.getColumnIndex(query.getColNames().get(i));
            Column column = tableSchema.getColumn(index);
            resultColumns.add(column);
            selectedColIndex.add(index);
        }

        Schema resultSchema = new Schema(resultColumns);
        Optional<RawConditionQuery> conditionOptional = query.getCondition();
        if(conditionOptional.isPresent()) {
            ConditionAnalyzer analyzer = new ConditionAnalyzer(tableSchema);
            Condition condition = analyzer.resolve(conditionOptional.get());
            if(!condition.isValidOperator()){
                throw new IllegalArgumentException("Invalid operator: " + condition.getOperator());
            }
            if(!condition.isValidValue()){
                throw new IllegalArgumentException("Invalid value: " + condition.getValue());
            }
            return new ResolvedSelectQuery(tableName,selectedColIndex,resultSchema,condition);
        }
        return new ResolvedSelectQuery(tableName,selectedColIndex,resultSchema);
    }
}
