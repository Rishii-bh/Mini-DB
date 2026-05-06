package MiniDB.StorageEngine;

import MiniDB.core.Column;
import MiniDB.core.Schema;
import MiniDB.core.Type;

import java.util.ArrayList;
import java.util.List;

public class SchemaSerializer {

    public String serialize(Schema schema) {
        List<Column> columns = schema.getColumns();
        StringBuilder result = new StringBuilder();
        for (Column column : columns) {
            result.append(column.getCol_name()).append(":").append(column.getType().name());
            result.append(System.lineSeparator());
        }
        return result.toString();
    }

    public Schema deserialize(List<String> columns){
        List<Column> schemaColumns = new ArrayList<Column>();
        for (String column : columns) {
            String[] parts = column.split(":");
            String colName = parts[0].trim();
            String colType = parts[1].trim().toUpperCase();

            Type type = switch (colType) {
                case "INT" -> Type.INT;
                case "TEXT" -> Type.TEXT;
                case "BOOL" -> Type.BOOL;
                default -> throw new StorageException("Invalid column type: " + colType);
            };
            schemaColumns.add(new Column(colName, type));

        }
        return new Schema(schemaColumns);
    }
}
