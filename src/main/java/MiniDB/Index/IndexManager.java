package MiniDB.Index;

import MiniDB.StorageEngine.*;
import MiniDB.core.Row;
import MiniDB.core.Schema;
import MiniDB.core.Type;
import MiniDB.core.Value;

import java.io.IOException;
import java.nio.file.DirectoryStream;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.*;

public class IndexManager {
    private final Map<IndexKey, InMemoryIndex> indexMap;
    private final PageFileStorage pageFileStorage;
    private final IndexBuilder indexBuilder;
    private final Path dbRoot;
    private final BinaryIndexSerializer serializer;
    private final IndexStorageEngine indexStorageEngine;
    private final Set<IndexKey> dirtyIndexes;
    private final Map<String ,Set<IndexKey>> loadedCatalog;
    private final Map<IndexKey , IndexMetaData> cachedIndexes;
    private final SchemaManager schemaManager;

    public IndexManager(Path dbRoot , PageFileStorage pageFileStorage, SchemaManager schemaManager) {
        this.pageFileStorage = pageFileStorage;

        cachedIndexes = new HashMap<>();
        this.schemaManager = schemaManager;
        indexBuilder = new IndexBuilder(pageFileStorage,schemaManager);
        indexMap = new HashMap<>();
        this.dbRoot = dbRoot;
        this.serializer = new BinaryIndexSerializer();
        this.indexStorageEngine = new IndexStorageEngine(dbRoot, new IndexCatalog(), serializer);
        this.dirtyIndexes = new HashSet<>();
        this.loadedCatalog = new HashMap<>();
    }
    public void loadAllIndexes() {
        if(!Files.exists(dbRoot)){
            return;
        }
        try(DirectoryStream<Path> tables = Files.newDirectoryStream(dbRoot)) {
            for (Path path : tables) {
                if(!Files.isDirectory(path)){
                    continue;
                }
                String tableName = path.getFileName().toString();
                List<IndexMetaData> tableIndexes = indexStorageEngine.loadCatalog(tableName);
                for(IndexMetaData indexMetaData : tableIndexes){
                    registerKey(indexMetaData);
                }
            }
        }catch (IOException e){
            throw new IndexException("Could not load Index Tables", e);
        }
    }

    private void registerKey(IndexMetaData indexMetaData){
        String tableName = indexMetaData.tableName();
        String colName = indexMetaData.colName();
        IndexKey key = new IndexKey(tableName, colName);
        loadedCatalog.computeIfAbsent(normalize(tableName), k -> new HashSet<>()).add(key);
        cachedIndexes.put(key, indexMetaData);
    }

    private String normalize(String value) {
        return value.toLowerCase(Locale.ROOT);
    }

    private void validateTableAndColumn(String tableName, String columnName) {
        if (tableName == null || columnName == null) {
            throw new IndexException("Table and column cannot be null");
        }

        Schema schema = schemaManager.getSchema(tableName);

        if (!schema.hasColumn(columnName)) {
            throw new IndexException(
                    "Unknown column " + columnName + " on table " + tableName
            );
        }
    }



    public void createIndex(String tableName, String columnName){
        validateTableAndColumn(tableName, columnName);
        IndexKey key = new IndexKey(tableName, columnName);
        try{
          if(cachedIndexes.containsKey(key)){
              throw new IndexException("Index already exists!");
          }
            Schema schema = schemaManager.getSchema(tableName);
            Type colType = schema.getColumn(columnName).getType();
            IndexMetaData metaData = indexStorageEngine.createMetadata(tableName ,columnName ,colType);
            InMemoryIndex indexedCol = indexBuilder.build(tableName, columnName);
            indexStorageEngine.createIndex(metaData ,indexedCol.indexEntries);
            indexStorageEngine.storeCatalog(tableName, metaData);
            loadedCatalog.computeIfAbsent(normalize(tableName), k -> new HashSet<>()).add(key);
            cachedIndexes.put(key, metaData);
            indexMap.put(key, indexedCol);
        }catch(IOException e){
            throw new IndexException("Could not create index", e);
        }
    }
    public InMemoryIndex loadOrGetIndex(IndexKey key ){
        InMemoryIndex existing = indexMap.get(key);
        if(existing != null){
            return existing;
        }
        IndexMetaData metaData = cachedIndexes.get(key);
        if(metaData == null){
            throw new IndexException("Specific index does not exist");
        }
        try{
            Map<Value, LinkedHashSet<RecordId>> idxMap = indexStorageEngine.readIndex(metaData);
            InMemoryIndex loaded = new InMemoryIndex(metaData.tableName(), metaData.colName(), idxMap);
            indexMap.put(key, loaded);
            return loaded;
        }catch(IOException e){
            throw new IndexException("Could not load index", e);
        }

    }

    public void flushDirtyIndexes(){
        try {
            for (IndexKey dirtyKey: new HashSet<>(dirtyIndexes)) {
                IndexMetaData metaData = cachedIndexes.get(dirtyKey);
                Map<Value, LinkedHashSet<RecordId>> indexEntries = loadOrGetIndex(dirtyKey).indexEntries;
                if(metaData == null || indexEntries == null){
                    throw new IndexException("DirtyIndex not loader");
                }
                indexStorageEngine.rewriteIndex(metaData,indexEntries);
            }
            dirtyIndexes.clear();
        }catch (IOException e){
            throw new IndexException("Could not rewrite index", e);
        }
    }


    public LinkedHashSet<RecordId> search(String tableName, String colName, Value valueKey) {
        validateTableAndColumn(tableName, colName);
        IndexKey key = new IndexKey(tableName, colName);
        if (!cachedIndexes.containsKey(key)) {
            throw new IndexException("Index does not exist");
        }
        InMemoryIndex indexedCol = loadOrGetIndex(key);
        return indexedCol.get(valueKey);
    }

    public void onInsert(String tableName, Row row, RecordId recordId) {
        if(!loadedCatalog.containsKey(tableName)){
            throw new IndexException("Table not found");
        }
        List<IndexKey> keys = loadedCatalog.get(tableName).stream().toList();
        Schema schema = schemaManager.getSchema(tableName);
        for(IndexKey key : keys){
           InMemoryIndex indexedCol = loadOrGetIndex(key);
           int columnIndex = schema.getColumnIndex(key.columnName());
            Value value = indexBuilder.extract(row,schema,columnIndex);
            if(indexedCol == null) {
                throw new IndexException("Column is Not indexed");
            }
            indexedCol.add(value,recordId);
            dirtyIndexes.add(key);

        }
        flushDirtyIndexes();
    }

    public void onDelete(String tableName, List<RowWithRecordId> rowWithRecordIds) {
        if(tableName == null || rowWithRecordIds == null ) {
            throw new IndexException("TableName and RowWithRecordIds cannot be null");
        }
        List<IndexKey> keys = loadedCatalog.get(tableName).stream().toList();
        Schema schema = schemaManager.getSchema(tableName);
        for(IndexKey key : keys) {
            InMemoryIndex indexedCol = loadOrGetIndex(key);
            String colName = key.columnName();
            int columnIndex = schema.getColumnIndex(colName);

           for(RowWithRecordId rowWithRecordId : rowWithRecordIds) {
               Row row = rowWithRecordId.row();
               RecordId rid = rowWithRecordId.getRecordId();
               Value value = indexBuilder.extract(row,schema,columnIndex);
               LinkedHashSet<RecordId> bucket = indexedCol.get(value);
               if(bucket == null) {
                   throw new IllegalStateException("Bucket at"+ rowWithRecordId.getRecordId() + " is null");
               }
               bucket.remove(rid);
               if(bucket.isEmpty()) {
                   indexedCol.delete(value);
               }
           }
           dirtyIndexes.add(key);
        }
        flushDirtyIndexes();
    }

     public boolean tableHasIndex(String tableName){
        return loadedCatalog.containsKey(tableName);
     }

     public boolean indexExists(String tableName, String colName){
        validateTableAndColumn(tableName, colName);
        IndexKey key = new IndexKey(tableName, colName);
        return cachedIndexes.containsKey(key);
     }

}
