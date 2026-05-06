package MiniDB.StorageEngine;

import MiniDB.core.Row;
import MiniDB.core.Schema;
import MiniDB.core.Type;

import java.util.ArrayList;
import java.util.List;
import java.util.stream.Collectors;

public class RowSerializer {
    private final String delimiter = "|";

    public String serialize(Row row) {
        return row.getRow().stream().map(this::serializeValue).collect(Collectors.joining(delimiter));
    }

    private String serializeValue(Object value) {
        if(value == null){
            throw new RuntimeException("Cannot serialize null value");
        }
        if(value instanceof String && ((String) value).contains(delimiter)){
            throw new StorageException("Text value cant contain delimiter" + delimiter);
        }
        return String.valueOf(value);
    }

    public Row deserialize(String row, Schema schema) {
        if (row == null || row.isBlank()) {
            throw new StorageException("Cannot deserialize empty row");
        }

        if (schema == null) {
            throw new StorageException("Cannot deserialize row without schema");
        }

        String[] values = row.split("\\|", -1);

        if (values.length != schema.getColumns().size()) {
            throw new StorageException(
                    "Stored row has "
                            + values.length
                            + " values, but schema expects "
                            + schema.getColumns().size()
            );
        }

        List<Object> valuesList = new ArrayList<>();

        for (int i = 0; i < values.length; i++) {
            Type expectedType = schema.getColumns().get(i).getType();
            Object parsedValue = parseValue(expectedType, values[i]);
            valuesList.add(parsedValue);
        }

        return new Row(valuesList);
    }

    private Object parseValue(Type type, String value) {
        return switch (type) {
            case INT -> parseInt(value);
            case TEXT -> value;
            case BOOL -> parseBool(value);
            default -> throw new RuntimeException("Unsupported type: " + type);
        };
    }

    private Integer parseInt(String value) {
        try {
            return Integer.parseInt(value);
        } catch (NumberFormatException e) {
            throw new StorageException("Invalid INT value in stored row: " + value);
        }
    }

    private Boolean parseBool(String value) {
        if (value.equals("true")) {
            return true;
        }

        if (value.equals("false")) {
            return false;
        }

        throw new StorageException("Invalid BOOL value in stored row: " + value);
    }
}
