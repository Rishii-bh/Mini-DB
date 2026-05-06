package MiniDB.Console;

import MiniDB.core.Row;
import MiniDB.core.Schema;
import MiniDB.query.QueryResult;
import MiniDB.query.results.CreateTableQueryResult;
import MiniDB.query.results.DeleteQueryResult;
import MiniDB.query.results.InsertQueryResult;
import MiniDB.query.results.SelectQueryResult;

public class ResultPrinter {
    public void print(QueryResult result) {
        if(result instanceof SelectQueryResult selectQueryResult){
            printResultSet(selectQueryResult);
            return;
        }
        if(result instanceof CreateTableQueryResult createTableQueryResult){
            System.out.println(createTableQueryResult.getMessage());
            return;
        }
        if(result instanceof InsertQueryResult insertQueryResult){
            System.out.println(insertQueryResult.getMessage());
            return;
        }
        if(result instanceof DeleteQueryResult deleteQueryResult){
            System.out.println(deleteQueryResult.getMessage());
            return;
        }
        throw new IllegalArgumentException(
                "Unsupported result type: " + result.getClass().getSimpleName()
        );
    }

    private void printResultSet(SelectQueryResult selectQueryResult) {
        Schema schema = selectQueryResult.getResultSchema();

        for (int i = 0; i < schema.size(); i++) {
            System.out.print(schema.getColumn(i).getCol_name());

            if (i < schema.size() - 1) {
                System.out.print(" | ");
            }
        }

        System.out.println();

        for (Row row : selectQueryResult.getResultRows()) {
            for (int i = 0; i < schema.size(); i++) {
                Object value = row.getValue(i);
                System.out.print(value);

                if (i < schema.size() - 1) {
                    System.out.print(" | ");
                }
            }

            System.out.println();
        }
    }
}
