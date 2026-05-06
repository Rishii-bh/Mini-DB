package MiniDB.query;

import MiniDB.StorageEngine.BinaryFileStorage;
import MiniDB.core.*;
import MiniDB.query.condition.Condition;
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
    private final BinaryFileStorage binaryFileStorage;

    public QueryExecutor(BinaryFileStorage binaryFileStorage) {
        this.binaryFileStorage = binaryFileStorage;
    }

    public SelectQueryResult executeSelectQuery(ResolvedSelectQuery resolvedQuery) {
        //original table and original schema
        String tableName = resolvedQuery.getTableName();
        Schema schema = binaryFileStorage.getSchema(tableName);
        Optional<Condition> conditionOptional = resolvedQuery.getResolvedCondition();
        if(conditionOptional.isEmpty()) {

            List<Row> resultRows = new ArrayList<>();
            for(int i=0; i<binaryFileStorage.getRows(tableName).size();i++){
                List<Object> values = new ArrayList<>();
                Row row = binaryFileStorage.getRows(tableName).get(i);
                for(int num : resolvedQuery.getSelectedColIndex()){
                    values.add(row.getValue(num));
                }
                resultRows.add(new Row(values));
            }
            //here i create a new resultSet with the given schema and resultRows
            //and return it
            return new SelectQueryResult(resolvedQuery.getResultSchema(), resultRows);

        }
        else {
           Condition condition = conditionOptional.get();
           //extracts the index of the condition column
           int index = schema.getColumnIndex(condition.getExpression1().getCol_name());
            List<Row> resultRows = new ArrayList<>();
            List<Row> originalRows = binaryFileStorage.getRows(tableName);
            for(int i=0;i<binaryFileStorage.getRows(tableName).size();i++){
//validating for each row before addinf the selected columns
                //normal select where eg: age >18 || name = "rishi" type checking
                //all types of checks found in the condition class
                Row row = originalRows.get(i);
                if(!condition.evaluate(row.getValue(index))){
                    continue;
                }
                List<Object> values = new ArrayList<>();
                for(int num : resolvedQuery.getSelectedColIndex()){
                    values.add(row.getValue(num));
                }
                resultRows.add(new Row(values));

            }
            return new SelectQueryResult(resolvedQuery.getResultSchema(), resultRows);

        }
    }

    public InsertQueryResult executeInsertQuery(ResolvedInsertQuery resolvedInsertQuery) {
        String tableName = resolvedInsertQuery.getTable();
        binaryFileStorage.insertRow(tableName,resolvedInsertQuery.getRow());
        return new InsertQueryResult(1, "1 row inserted");
    }

    public CreateTableQueryResult executeCreateQuery(ResolvedCreateTableQuery resolvedCreateTableQuery) {
        Schema schema = new Schema(resolvedCreateTableQuery.getColumns());
        binaryFileStorage.createTable(resolvedCreateTableQuery.getTableName(), schema);
        return new CreateTableQueryResult(resolvedCreateTableQuery.getTableName(), schema.size());
    }

    public DeleteQueryResult executeDeleteQuery(ResolvedDeleteQuery resolvedDeleteQuery) {
        String tableName = resolvedDeleteQuery.getTableName();
        int tableRowCount = binaryFileStorage.getRows(tableName).size();
        Condition condition = resolvedDeleteQuery.getCondition();
        int colToCheckIndex = resolvedDeleteQuery.getColToCheckIndex();
        Integer numOfRowsDeleted =0;
        List<Row> resultRows = new ArrayList<>();
        List<Row> allRows = binaryFileStorage.getRows(tableName);
        for(Row row : allRows){
            if(condition.evaluate(row.getValue(colToCheckIndex))){
                numOfRowsDeleted++;
                continue;
            }
            resultRows.add(row);
        }
        binaryFileStorage.replaceRows(tableName,resultRows);

        return new DeleteQueryResult(tableName, numOfRowsDeleted);
    }


}
