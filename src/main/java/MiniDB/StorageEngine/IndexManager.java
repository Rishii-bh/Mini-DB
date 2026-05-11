package MiniDB.StorageEngine;

import MiniDB.core.Row;
import MiniDB.core.Schema;
import MiniDB.core.Value;

import java.util.*;
import java.util.stream.Collectors;

public class IndexManager {
    private final Map<IndexKey, InMemoryIndex> indexMap;
    private final PageFileStorage pageFileStorage;
    private final IndexBuilder indexBuilder;
    public IndexManager(PageFileStorage pageFileStorage) {
        this.pageFileStorage = pageFileStorage;
        indexBuilder = new IndexBuilder(pageFileStorage);
        indexMap = new HashMap<>();
    }

    public void createIndex(String tableName, String columnName) {
        IndexKey key = new IndexKey(tableName, columnName);
        if(indexMap.containsKey(key)) {
            return;
        }
        // this  next line will throw if table not found or problems with column
        InMemoryIndex indexedCol = indexBuilder.build(tableName, columnName);
        indexMap.put(key, indexedCol);
    }

    public boolean hasIndexKey(String tableName, String columnName) {
        IndexKey key = new IndexKey(tableName, columnName);
        return indexMap.containsKey(key);
    }

    public boolean tableHasIndex(String tableName) {
        for(IndexKey key : indexMap.keySet()) {
            if(key.tableName().equals(tableName)) {
                return true;
            }
        }
        return false;
    }


    public LinkedHashSet<RecordId> search(String tableName, String colName, Value valueKey) {
        IndexKey indexKey = new IndexKey(tableName, colName);
        if(hasIndexKey(tableName,colName)) {
            InMemoryIndex indexedCol = indexMap.get(indexKey);
            return indexedCol.get(valueKey);
        }
        throw new IndexException("Column '" + colName + "' is Not indexed");
    }

    public void addValueToIndexWhenInserting(String tableName, String columnName, Row row,RecordId recordId) {
        IndexKey indexKey = new IndexKey(tableName, columnName);
        Schema schema = pageFileStorage.getSchema(tableName);
        int columnIndex = schema.getColumnIndex(columnName);
        Value value = indexBuilder.extract(row,schema,columnIndex);
        InMemoryIndex indexedCol = indexMap.get(indexKey);
        if(indexedCol == null) {
            throw new IndexException("Column '" + columnName + "' is Not indexed");
        }
        indexedCol.add(value,recordId);
    }

    public void reBuildIndex(String tableName){
        List<IndexKey> keysToRebuild = new ArrayList<>();
        for(IndexKey key : indexMap.keySet()) {
            if(key.tableName().equals(tableName)) {
                keysToRebuild.add(key);
                indexMap.remove(key);
            }
        }
        for(IndexKey key : keysToRebuild) {
            String table = key.tableName();
            String columnName = key.columnName();
            createIndex(table,columnName);
        }
    }

    public void deleteRecordIds(String tableName, List<RowWithRecordId> rowWithRecordIds) {
        if(tableName == null || rowWithRecordIds == null ) {
            throw new IndexException("TableName and RowWithRecordIds cannot be null");
        }
        List<IndexKey> indexKeys = new ArrayList<>();
        for(IndexKey key : indexMap.keySet()) {
            if(key.tableName().equals(tableName)) {
                indexKeys.add(key);
            }
        }
        Schema schema = pageFileStorage.getSchema(tableName);
        for(IndexKey key : indexKeys) {
            InMemoryIndex indexedCol = indexMap.get(key);
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
        }

    }
}
