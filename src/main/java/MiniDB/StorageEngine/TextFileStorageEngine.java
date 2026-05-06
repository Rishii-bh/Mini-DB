package MiniDB.StorageEngine;

import MiniDB.core.Row;
import MiniDB.core.Schema;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardOpenOption;
import java.util.ArrayList;
import java.util.List;

public class TextFileStorageEngine implements StorageEngine {
    private final String SCHEMA_FILE_NAME = "schema.meta";
    private final String ROWS_FILE_NAME = "rows.data";
    private final RowSerializer rowSerializer;
    private final SchemaSerializer schemaSerializer;

    private final Path rootDir;

    public TextFileStorageEngine(RowSerializer rowSerializer, SchemaSerializer schemaSerializer, Path rootDir) {
        this.rowSerializer = rowSerializer;
        this.schemaSerializer = schemaSerializer;
        if (rootDir == null) {
            throw new StorageException("rootDir should be provided");
        }
        this.rootDir = rootDir;
        try {
            Files.createDirectories(rootDir);
        } catch (IOException E) {
            throw new StorageException(E.getMessage());
        }
    }

    private Path tableDir(String tableName) {
        validateTableName(tableName);
        return rootDir.resolve(tableName);
    }

    private Path schemaFile(String tableName) {
        return tableDir(tableName).resolve(SCHEMA_FILE_NAME);
    }

    private Path rowsFile(String tableName) {
        return tableDir(tableName).resolve(ROWS_FILE_NAME);
    }

    private void validateTableName(String tableName) {
        if (tableName == null || tableName.isEmpty()) {
            throw new StorageException("tableName should be provided");
        }
        if (!tableName.matches("[a-zA-Z_][a-zA-Z0-9_]*")) {
            throw new StorageException("Invalid table name: " + tableName);
        }
    }

    @Override
    public void createTable(String table_name, Schema schema) {
        Path tableDir = tableDir(table_name);
        Path schemaFiles = schemaFile(table_name);
        Path rowsFiles = rowsFile(table_name);

        try {
            if (Files.exists(tableDir)) {
                throw new StorageException("Table " + table_name + " already exists");
            }
            Files.createDirectories(tableDir);
            String serializedSchema = schemaSerializer.serialize(schema);
            Files.writeString(schemaFiles, serializedSchema);
            Files.createFile(rowsFiles);

        } catch (IOException e) {
            throw new StorageException(e.getMessage());
        }
    }

    @Override
    public boolean tableExists(String table_name) {
        Path tableDir = tableDir(table_name);
        return Files.exists(tableDir);
    }

    @Override
    public Schema getSchema(String table_name) {
        Path tableDir = tableDir(table_name);
        if (!Files.isDirectory(tableDir)) {
            throw new StorageException("Table " + table_name + " is not a directory");
        }
        Path schemaFile = schemaFile(table_name);
        try {
            if (!Files.exists(schemaFile)) {
                throw new StorageException("Couldnt Find Schema File in director" + schemaFile);
            }
            List<String> lines = Files.readAllLines(schemaFile);
            return schemaSerializer.deserialize(lines);
        } catch (IOException e) {
            throw new StorageException("Couldnt Read Schema File in director" + schemaFile, e);
        }
    }

    @Override
    public List<Row> getRows(String table_name) {
        Path tableDir = tableDir(table_name);
        if (!Files.isDirectory(tableDir)) {
            throw new StorageException("Table " + table_name + " is not a directory");
        }
        Path rowsFile = rowsFile(table_name);
        Path schemaFiles = schemaFile(table_name);
        try {
            if (!Files.exists(rowsFile)) {
                throw new StorageException("Couldnt Find Rows File in directory" + rowsFile);
            }
            if (!Files.exists(schemaFiles)) {
                throw new StorageException("Couldnt Find Schema File in directory" + schemaFiles);
            }
            List<String> rowLines = Files.readAllLines(rowsFile);
            Schema schema = getSchema(table_name);
            List<Row> rows = new ArrayList<>();
            for (String rowLine : rowLines) {
                if(rowLine.isBlank()){
                    continue;
                }
                rows.add(rowSerializer.deserialize(rowLine, schema));
            }
            return rows;
        } catch (IOException e) {
            throw new StorageException("Couldnt Read Rows File in director" + rowsFile, e);
        }
    }

    @Override
    public void insertRow(String table_name, Row row) {
        Path tableDir = tableDir(table_name);
        if (!Files.isDirectory(tableDir)) {
            throw new StorageException("Table " + table_name + " is not a directory");
        }
        if(row == null){
            throw new StorageException("Cant insert null row");
        }
        Path rowsFile = rowsFile(table_name);
        try {
            if (!Files.exists(rowsFile)) {
                throw new StorageException("Couldnt Find Rows File in directory" + rowsFile);
            }
            String serializedRow = rowSerializer.serialize(row);
            Files.writeString(rowsFile, serializedRow + System.lineSeparator(), StandardOpenOption.APPEND);

        } catch (IOException e) {
            throw new StorageException("Row couldnt be written in directory" + rowsFile, e);
        }

    }

    @Override
    public void replaceRows(String table_name, List<Row> rows) {
        Path tableDir = tableDir(table_name);
        if (!Files.isDirectory(tableDir)) {
            throw new StorageException("Table " + table_name + " is not a directory");
        }
        if(rows == null){
            throw new StorageException("Cant replace null rows");
        }
        Path rowsFile = rowsFile(table_name);
       StringBuilder sb = new StringBuilder();
       for (Row row : rows) {
           if(row == null){
               throw new StorageException("Cant rewrite null row");
           }
           String serializedRow = rowSerializer.serialize(row);
           sb.append(serializedRow).append(System.lineSeparator());
       }
       try {
           Files.writeString(rowsFile,sb,StandardOpenOption.TRUNCATE_EXISTING, StandardOpenOption.WRITE);
       } catch (IOException e) {
           throw new StorageException("Row couldnt be written in directory" + rowsFile, e);
       }
    }
}
