package MiniDB.query;

import MiniDB.StorageEngine.BinaryFileStorage;
import MiniDB.StorageEngine.IndexManager;
import MiniDB.StorageEngine.PageFileStorage;
import MiniDB.StorageEngine.RecordId;
import MiniDB.core.*;
import MiniDB.query.condition.Condition;
import MiniDB.query.condition.Operator;
import MiniDB.query.resolved.ResolvedCreateTableQuery;
import MiniDB.query.resolved.ResolvedDeleteQuery;
import MiniDB.query.resolved.ResolvedInsertQuery;
import MiniDB.query.resolved.ResolvedSelectQuery;
import MiniDB.query.results.CreateTableQueryResult;
import MiniDB.query.results.DeleteQueryResult;
import MiniDB.query.results.InsertQueryResult;
import MiniDB.query.results.SelectQueryResult;

import java.util.ArrayList;
import java.util.List;
import java.util.Optional;

public class QueryExecutor {
    private final PageFileStorage pageFileStorage;
    private final IndexManager indexManager;

    public QueryExecutor(PageFileStorage pageFileStorage,IndexManager indexManager) {

        this.pageFileStorage = pageFileStorage;
        this.indexManager = indexManager;

    }

    public SelectQueryResult executeSelectQuery(ResolvedSelectQuery resolvedQuery) {
        //original table and original schema
        String tableName = resolvedQuery.getTableName();
        Schema schema = pageFileStorage.getSchema(tableName);
        Optional<Condition> conditionOptional = resolvedQuery.getResolvedCondition();
        if (conditionOptional.isEmpty()) {

            List<Row> resultRows = new ArrayList<>();
            for (int i = 0; i < pageFileStorage.getRows(tableName).size(); i++) {
                List<Object> values = new ArrayList<>();
                Row row = pageFileStorage.getRows(tableName).get(i);
                for (int num : resolvedQuery.getSelectedColIndex()) {
                    values.add(row.getValue(num));
                }
                resultRows.add(new Row(values));
            }
            //here i create a new resultSet with the given schema and resultRows
            //and return it
            return new SelectQueryResult(resolvedQuery.getResultSchema(), resultRows);

        } else {
            Condition condition = conditionOptional.get();
            //extracts the index of the condition column
            String colInCondition = condition.getExpression1().getCol_name();
            int index = schema.getColumnIndex(colInCondition);
            List<Row> resultRows = new ArrayList<>();
            if (condition.getOperator()== Operator.EQUALTO &&
                    indexManager.hasIndexKey(tableName, colInCondition)) {
                Type type = condition.getExpression1().getType();
                Value value = new Value(type, condition.getValue());
                List<RecordId> recordIds = indexManager.search(tableName, colInCondition, value);
                List<Row> rowsFromRecordId = new ArrayList<>();
                for (RecordId recordId : recordIds) {
                    rowsFromRecordId.add(pageFileStorage.getRowByRecordId(tableName, recordId));
                }
                List<Object> values = new ArrayList<>();
                for (Row row : rowsFromRecordId) {
                    for (int num : resolvedQuery.getSelectedColIndex()) {
                        values.add(row.getValue(num));
                    }
                    resultRows.add(new Row(values));
                }
                return new SelectQueryResult(resolvedQuery.getResultSchema(), resultRows);
            }
            List<Row> originalRows = pageFileStorage.getRows(tableName);
            for (int i = 0; i < pageFileStorage.getRows(tableName).size(); i++) {
//validating for each row before addinf the selected columns
                //normal select where eg: age >18 || name = "rishi" type checking
                //all types of checks found in the condition class
                Row row = originalRows.get(i);
                if (!condition.evaluate(row.getValue(index))) {
                    continue;
                }
                List<Object> values = new ArrayList<>();
                for (int num : resolvedQuery.getSelectedColIndex()) {
                    values.add(row.getValue(num));
                }
                resultRows.add(new Row(values));

            }
            return new SelectQueryResult(resolvedQuery.getResultSchema(), resultRows);

        }
    }

    public InsertQueryResult executeInsertQuery(ResolvedInsertQuery resolvedInsertQuery) {
        String tableName = resolvedInsertQuery.getTable();
        RecordId recordId = pageFileStorage.insertRow(tableName,resolvedInsertQuery.getRow());
        Schema schema = pageFileStorage.getSchema(tableName);
        for(Column column : schema.getColumns()) {
            String colName = column.getCol_name();
            if(indexManager.hasIndexKey(tableName, colName)) {
                indexManager.addValueToIndexWhenInserting(tableName,colName,resolvedInsertQuery.getRow(),recordId);
                break;
            }
        }
        return new InsertQueryResult(1, "1 row inserted");
    }

    public CreateTableQueryResult executeCreateQuery(ResolvedCreateTableQuery resolvedCreateTableQuery) {
        Schema schema = new Schema(resolvedCreateTableQuery.getColumns());
        pageFileStorage.createTable(resolvedCreateTableQuery.getTableName(), schema);
        return new CreateTableQueryResult(resolvedCreateTableQuery.getTableName(), schema.size());
    }

    public DeleteQueryResult executeDeleteQuery(ResolvedDeleteQuery resolvedDeleteQuery) {
        String tableName = resolvedDeleteQuery.getTableName();
        int tableRowCount = pageFileStorage.getRows(tableName).size();
        Condition condition = resolvedDeleteQuery.getCondition();
        int colToCheckIndex = resolvedDeleteQuery.getColToCheckIndex();
        Integer numOfRowsDeleted =0;
        List<Row> resultRows = new ArrayList<>();
        List<Row> allRows = pageFileStorage.getRows(tableName);
        for(Row row : allRows){
            if(condition.evaluate(row.getValue(colToCheckIndex))){
                numOfRowsDeleted++;
                continue;
            }
            resultRows.add(row);
        }
        pageFileStorage.replaceRows(tableName,resultRows);
        indexManager.reBuildIndex(tableName);

        return new DeleteQueryResult(tableName, numOfRowsDeleted);
    }


}
