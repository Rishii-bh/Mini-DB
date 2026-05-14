package MiniDB.StorageEngine;

import MiniDB.core.Row;
import MiniDB.core.Schema;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.List;
import java.util.Optional;

public class PageFileStorage  {
    private final String SCHEMA_FILE_NAME = "schema.meta";
    private final String TABLE_FILE_NAME = "table.dat";
    private final BinaryRowSerializer rowSerializer;
    private final SchemaSerializer schemaSerializer;
    private final SchemaManager schemaManager;

    private final Path rootDir;
    public PageFileStorage(SchemaManager schemaManager, Path rootDir) {
        this.schemaManager = schemaManager;
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

    private Path tablesFile(String tableName) {
        return tableDir(tableName).resolve(TABLE_FILE_NAME);
    }

    private void validateTableName(String tableName) {
        if (tableName == null || tableName.isEmpty()) {
            throw new StorageException("tableName should be provided");
        }
        if (!tableName.matches("[a-zA-Z_][a-zA-Z0-9_]*")) {
            throw new StorageException("Invalid table name: " + tableName);
        }
    }

    private PageFile getPageFile(String tableName) {
        return new PageFile(tablesFile(tableName));
    }


    public void createTable(String table_name, Schema schema) {
        Path tableDir = tableDir(table_name);
        Path schemaFiles = schemaFile(table_name);
        Path tablesFiles = tablesFile(table_name);

        try {
            if (Files.exists(tableDir)) {
                throw new StorageException("Table " + table_name + " already exists");
            }
            Files.createDirectories(tableDir);
            String serializedSchema = schemaSerializer.serialize(schema);
            Files.writeString(schemaFiles, serializedSchema);
            Files.createFile(tablesFiles);
            schemaManager.registerSchema(table_name, schema);

        } catch (IOException e) {
            throw new StorageException(e.getMessage());
        }
    }


    public boolean tableExists(String table_name) {
        Path tableDir = tableDir(table_name);
        return Files.exists(tableDir);
    }


    public Schema getSchema(String table_name) {
        Path tableDir = tableDir(table_name);
        Path schemaFiles = schemaFile(table_name);
        ensureTableDirExists(tableDir);
        ensureSchemaFileExists(schemaFiles);
        try{
            List<String> lines = Files.readAllLines(schemaFiles);
            return schemaSerializer.deserialize(lines);
        }catch (IOException e){
            throw new StorageException("Could not read schema File", e);
        }
    }

    public List<Row> getRows(String table_name) {
        Path tableDir = tableDir(table_name);
        Path schemaFiles = schemaFile(table_name);
        Path tablesFiles = tablesFile(table_name);
        ensureTableDirExists(tableDir);
        ensureSchemaFileExists(schemaFiles);
        ensureTableFileExists(tablesFiles);
        Schema schema = getSchema(table_name);
        PageFile pageFile = getPageFile(table_name);
        List<Row> rows = new ArrayList<>();
        try{
            int numOfPages = pageFile.getPageCount();
            for (int i = 0; i < numOfPages; i++) {
                Page page = pageFile.readPage(i);
                int slotCount = page.getSlotCount();
                for (int j = 0; j < slotCount; j++) {
                    if(page.isDeletedSlot(j)){
                        continue;
                    }
                    byte[] rowBytes = page.getRowBytes(j);
                    Row row = rowSerializer.deserialize(rowBytes, schema);
                    rows.add(row);
                }
            }


        } catch (RuntimeException e) {
            throw new StorageException("Could not read Page File", e);
        }
        return rows;
    }

    public RecordId insertRow(String table_name, Row row) {
        Path tableDir = tableDir(table_name);
        Path schemaFiles = schemaFile(table_name);
        Path tablesFiles = tablesFile(table_name);
        ensureTableDirExists(tableDir);
        ensureSchemaFileExists(schemaFiles);
        ensureTableFileExists(tablesFiles);
        Schema schema = getSchema(table_name);
        try {
            PageFile pageFile = getPageFile(table_name);
            int numOfPages = pageFile.getPageCount();
            byte[] rowBytes = rowSerializer.serialize(row,schema);
            for (int i = 0; i < numOfPages; i++) {
                int slotId =-1;
                    Page page = pageFile.readPage(i);
                    if(page.reusableSlotExists(rowBytes.length)){
                        slotId = page.tryInsertInDeletedSlot(rowBytes);
                    }
                    else {
                        slotId =page.tryInsert(rowBytes);
                    }
                    if(slotId != -1){
                        pageFile.writePage(page, i);
                        return new RecordId(i,slotId);
                    }

            }
           Page page = Page.createEmptyPage();
            int slotId = page.tryInsert(rowBytes);
            if(slotId == -1){
                throw new StorageException("Row Size too big for a single page");
            }
            pageFile.appendPage(page);
            return new RecordId(0,slotId);


        } catch (RuntimeException e) {
            throw new StorageException("Could not insert Row", e);
        }
    }


//    public void replaceRows(String table_name, List<Row> rows) {
//         Path tableDir = tableDir(table_name);
//         Path schemaFiles = schemaFile(table_name);
//         Path tablesFiles = tablesFile(table_name);
//         ensureTableDirExists(tableDir);
//         ensureSchemaFileExists(schemaFiles);
//         ensureTableFileExists(tablesFiles);
//         PageFile pageFile = getPageFile(table_name);
//         try{
//             pageFile.truncate();
//             for (Row row : rows) {
//                 insertRow(table_name, row);
//             }
//         } catch (RuntimeException e) {
//             throw new StorageException("Could not replace Rows", e);
//         }
//    }

    public Optional<Row> getRowByRecordId(String table_name, RecordId recordId) {
        if(recordId == null|| recordId.PageNo() < 0 || recordId.SlotId() < 0){
            throw new StorageException("RecordId is null or unspecified value");
        }
        Path tableDir = tableDir(table_name);
        Path schemaFiles = schemaFile(table_name);
        Path tablesFiles = tablesFile(table_name);
        ensureTableDirExists(tableDir);
        ensureSchemaFileExists(schemaFiles);
        ensureTableFileExists(tablesFiles);
        Schema schema = getSchema(table_name);
        try{
            PageFile pageFile = getPageFile(table_name);
            Page page = pageFile.readPage(recordId.PageNo());
            Optional<byte[]> rowBytesOptional = page.readLiveRowBytes(recordId.SlotId());
            if(rowBytesOptional.isEmpty()){
                return Optional.empty();
            }
            byte[] rowBytes = rowBytesOptional.get();
            return Optional.of(rowSerializer.deserialize(rowBytes,schema));

        }catch (RuntimeException e){
            throw new StorageException("Could not get Row", e);
        }

    }

    public List<RowWithRecordId> scanRows(String table_name) {
        Path tableDir = tableDir(table_name);
        Path schemaFiles = schemaFile(table_name);
        Path tablesFiles = tablesFile(table_name);
        ensureTableDirExists(tableDir);
        ensureSchemaFileExists(schemaFiles);
        ensureTableFileExists(tablesFiles);
        Schema schema = getSchema(table_name);
        List<RowWithRecordId> rows = new ArrayList<>();
        try{
            PageFile pageFile = getPageFile(table_name);
            int pageCount = pageFile.getPageCount();
            for (int i = 0; i < pageCount; i++) {
                Page page = pageFile.readPage(i);
                for(int slotId = 0; slotId < page.getSlotCount(); slotId++){
                    if(page.isDeletedSlot(slotId)){
                        continue;
                    }
                    byte[] rowBytes = page.getRowBytes(slotId);
                    Row row = rowSerializer.deserialize(rowBytes,schema);
                    rows.add(new RowWithRecordId(row,new RecordId(i,slotId)));
                }
            }
        }catch (RuntimeException e){
            throw new StorageException("Could not get Rows", e);
        }
        return rows;
    }

    public void markDelete(String table_name, List<RowWithRecordId> rows) {
        Path tableDir = tableDir(table_name);
        Path schemaFiles = schemaFile(table_name);
        Path tablesFiles = tablesFile(table_name);
        ensureTableDirExists(tableDir);
        ensureSchemaFileExists(schemaFiles);
        ensureTableFileExists(tablesFiles);

        try{
            PageFile pageFile = getPageFile(table_name);
            for(RowWithRecordId row : rows){
                int pageNo = row.recordId().PageNo();
                Page page = pageFile.readPage(pageNo);
                page.markSlotAsDeleted(row.recordId().SlotId());
                pageFile.writePage(page,pageNo);
            }
        } catch (RuntimeException e) {
            throw new StorageException("Coudn't delete Rows", e);
        }
    }


//HELPERS
    private void ensureTableDirExists(Path tableDir) {
        if(!Files.exists(tableDir)) {
            throw new StorageException("Table directory does not exist: " + tableDir);
        }
    }

    private void ensureSchemaFileExists(Path schemaFile) {
        if(!Files.exists(schemaFile)) {
            throw new StorageException("Schema file does not exist: " + schemaFile);
        }
    }

    private void ensureTableFileExists(Path tableFile) {
        if(!Files.exists(tableFile)) {
            throw new StorageException("Table file does not exist: " + tableFile);
        }
    }

}
