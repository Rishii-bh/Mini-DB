package MiniDB.StorageEngine;

import MiniDB.core.Row;
import MiniDB.core.Schema;

import java.io.*;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardOpenOption;
import java.util.ArrayList;
import java.util.List;

public class BinaryFileStorage implements StorageEngine {
    private final String SCHEMA_FILE_NAME = "schema.meta";
    private final String ROWS_FILE_NAME = "rows.bin";
    private final BinaryRowSerializer rowSerializer;
    private final SchemaSerializer schemaSerializer;

    private final Path rootDir;

    public BinaryFileStorage(Path rootDir) {
        if(rootDir == null) {
            throw new StorageException("Root directory not specified!");
        }
        this.rootDir = rootDir;
        this.rowSerializer = new BinaryRowSerializer();
        this.schemaSerializer = new SchemaSerializer();
        try{
            Files.createDirectories(rootDir);
        } catch (IOException e) {
            throw new StorageException("Could not create root directory!", e);
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
        Schema schema = getSchema(table_name);
        List<Row> rows = new ArrayList<>();
        try {
            if (!Files.exists(rowsFile)) {
                throw new StorageException("Couldnt Find Rows File in directory" + rowsFile);
            }
            if (!Files.exists(schemaFiles)) {
                throw new StorageException("Couldnt Find Schema File in directory" + schemaFiles);
            }
            InputStream fileIn = Files.newInputStream(rowsFile);
            DataInputStream data = new DataInputStream(fileIn);

            while(true){
                int rowLength;
                try{
                    rowLength = data.readInt();
                }catch(EOFException e){
                    break;
                }
                if(rowLength < 0){
                    throw new StorageException("Invalid row length: " + rowLength);
                }
                byte[] rowBytes = new byte[rowLength];
                data.readFully(rowBytes);
                Row row = rowSerializer.deserialize(rowBytes,schema);
                rows.add(row);
            }
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
        return rows;
    }

    @Override
    public void insertRow(String table_name, Row row) {
        Path tableDir = tableDir(table_name);
        if (!Files.isDirectory(tableDir)) {
            throw new StorageException("Table " + table_name + " is not a directory");
        }
        if(row == null){
            throw new StorageException("Row is null");
        }
        Path rowsFile = rowsFile(table_name);
        Schema schema = getSchema(table_name);
        byte[] rowBytes = rowSerializer.serialize(row,schema);
        try{
            if (!Files.exists(rowsFile)) {
                throw new StorageException("Couldnt Find Rows File in directory" + rowsFile);
            }
            OutputStream fileOut = Files.newOutputStream(rowsFile , StandardOpenOption.APPEND);
            DataOutputStream dataOut = new DataOutputStream(fileOut);
            dataOut.writeInt(rowBytes.length);
            dataOut.write(rowBytes);



        } catch (IOException e) {
            throw new StorageException("Couldnt write row to file" , e);
        }

    }

    @Override
    public void replaceRows(String table_name, List<Row> rows) {
        Path tableDir = tableDir(table_name);
        if (!Files.isDirectory(tableDir)) {
            throw new StorageException("Table " + table_name + " is not a directory");
        }
        if(rows == null){
            throw new StorageException("Rows is null");
        }
        Path rowsFile = rowsFile(table_name);
        if (!Files.isDirectory(tableDir)) {
            throw new StorageException("Table " + table_name + " is not a directory");
        }
        Schema schema = getSchema(table_name);
        try{
            OutputStream fileOut = Files.newOutputStream(rowsFile , StandardOpenOption.TRUNCATE_EXISTING,
                    StandardOpenOption.WRITE);
            DataOutputStream dataOut = new DataOutputStream(fileOut);

            for(Row row : rows){
                if(row == null){
                    throw new StorageException("Row is null");
                }
                byte[] rowBytes = rowSerializer.serialize(row,schema);
                dataOut.writeInt(rowBytes.length);
                dataOut.write(rowBytes);
            }
        } catch (IOException e) {
            throw new StorageException("Couldnt write row to file" , e);
        }

    }
}
