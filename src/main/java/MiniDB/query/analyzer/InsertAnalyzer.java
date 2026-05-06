package MiniDB.query.analyzer;

import MiniDB.StorageEngine.BinaryFileStorage;
import MiniDB.StorageEngine.TextFileStorageEngine;
import MiniDB.core.*;
import MiniDB.query.rawqueries.InsertQuery;
import MiniDB.query.resolved.ResolvedInsertQuery;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

public class InsertAnalyzer {
    private final BinaryFileStorage binaryFileStorage;

    public InsertAnalyzer(BinaryFileStorage binaryFileStorage) {
        this.binaryFileStorage = binaryFileStorage;
    }


    public ResolvedInsertQuery resolve(InsertQuery query) {
        String tableName = query.getTable_name();
        if(tableName == null||tableName.isEmpty()) {
            throw new RuntimeException("Table name is null or empty");
        }
        if(!binaryFileStorage.tableExists(tableName)) {
           throw new RuntimeException("Table " + tableName + " does not exist");
       }
        Schema schema = binaryFileStorage.getSchema(tableName);
        if(query.getCol_names().size() != schema.size() ||
        query.getValues().size() != schema.size()) {
            throw new IllegalArgumentException("Inserted columns and values do not match Table Schema");
        }
        int originalSchemaSize = schema.size();
        //creating a list of nulls with size of originalSchema
        //creating a seen for duplicates in col_names
        List<Object> orderedValues = new ArrayList<>(Collections.nCopies(originalSchemaSize, null));
        boolean[] seen = new boolean[originalSchemaSize];

        for (int i = 0; i < originalSchemaSize; i++) {
            String queryCol = query.getCol_names().get(i);
            Object value = query.getValues().get(i);

            int schemaIndex = schema.getColumnIndex(queryCol);

            if (seen[schemaIndex]) {
                throw new IllegalArgumentException("Duplicate column in INSERT: " + queryCol);
            }

            seen[schemaIndex] = true;

            Type expectedType = schema.getColumn(schemaIndex).getType();

            if (!matchTypes(expectedType, value)) {
                throw new IllegalArgumentException(
                        "Inserted value for column " + queryCol + " does not match type " + expectedType
                );
            }

            orderedValues.set(schemaIndex, value);
        }
        Row row = new Row(orderedValues);
        return new ResolvedInsertQuery(tableName, row);
    }


    private boolean matchTypes(Type type , Object object) {
        return switch (type) {
            case INT -> object instanceof Integer;
            case TEXT -> object instanceof String;
            case BOOL -> object instanceof Boolean;
            case REAL -> object instanceof Double;
        };
    }
}
