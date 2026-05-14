package MiniDB.Index;

import MiniDB.StorageEngine.RecordId;
import MiniDB.core.Type;
import MiniDB.core.Value;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.LinkedHashMap;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;

public class IndexStorageEngine {
    private static final String INDEX_DIR_NAME = "indexes";
    private static final String INDEXES_META_NAME = "indexes.meta";
    private static final String INDEX_FILE_EXTENSION = ".idx";

    private final Path dbRoot;
    private final IndexCatalog catalog;
    private final BinaryIndexSerializer binaryIndexSerializer;

    public IndexStorageEngine(Path dbRoot, IndexCatalog catalog , BinaryIndexSerializer binaryIndexSerializer) {
       if(dbRoot == null){
           throw new IndexException("dbRoot is null");
       }
       if(catalog == null){
           throw new IndexException("catalog is null");
       }
       if(binaryIndexSerializer == null){
           throw new IndexException("binaryIndexSerializer is null");
       }
       this.dbRoot = dbRoot;
       this.catalog = catalog;
       this.binaryIndexSerializer = binaryIndexSerializer;
    }

    public List<IndexMetaData> loadCatalog(String table_name) throws IOException {
        Path catalogPath = resolveCatalogPath(table_name);
        return catalog.read(catalogPath);
    }


    public void storeCatalog(String table_name, IndexMetaData metaData) throws IOException {
        Path catalogPath = resolveCatalogPath(table_name);
        catalog.write(catalogPath,metaData);
    }

    public IndexMetaData createMetadata(String tableName, String columnName, Type columnType) {
        String fileName = columnName + INDEX_FILE_EXTENSION;

        return new IndexMetaData(
                tableName,
                columnName,
                columnType,
                IndexType.FLAT,
                fileName
        );
    }

    public void createIndex(IndexMetaData metaData , Map<Value , LinkedHashSet<RecordId>> indexMap) throws IOException {
        if(metaData == null){
            throw new IndexException("metaData is null");
        }
        if(indexMap == null){
            throw new IndexException("indexMap is null");
        }
        Path indexPath = resolveIndexPath(metaData);

        try{
            Files.createDirectories(indexPath.getParent());
            IndexPageFile indexPageFile = new IndexPageFile(indexPath);
            IndexPage indexPage = IndexPage.createIndexPage();
            int pageNo =0;

            for(Map.Entry<Value , LinkedHashSet<RecordId>> entry : indexMap.entrySet()){
                Value key = entry.getKey();
                for(RecordId recordId : entry.getValue()){
                    byte[] indexBytes = binaryIndexSerializer.serialize(key , recordId);
                    boolean success = indexPage.tryInsert(indexBytes);
                    if(!success){
                        indexPageFile.writePage(indexPage , pageNo);
                        pageNo++;
                        indexPage = IndexPage.createIndexPage();
                        success = indexPage.tryInsert(indexBytes);
                        if(!success){
                            throw new IndexException("Index is too large for a single page");
                        }
                    }
                }
            }
            indexPageFile.writePage(indexPage , pageNo);
        }catch(IOException e){
            throw new IndexException("Could not create index", e);
        }
    }

    public void rewriteIndex(IndexMetaData metaData , Map<Value , LinkedHashSet<RecordId>> indexMap) throws IOException {
        if(metaData == null){
            throw new IndexException("metaData is null");
        }
        if(indexMap == null){
            throw new IndexException("indexMap is null");
        }
        Path indexPath = resolveIndexPath(metaData);
        try{
            Files.createDirectories(indexPath.getParent());
            IndexPageFile indexPageFile = new IndexPageFile(indexPath);
            indexPageFile.truncate();
            IndexPage indexPage = IndexPage.createIndexPage();
            int pageNo =0;

            for(Map.Entry<Value , LinkedHashSet<RecordId>> entry : indexMap.entrySet()){
                Value key = entry.getKey();
                for(RecordId recordId : entry.getValue()){
                    byte[] indexBytes = binaryIndexSerializer.serialize(key , recordId);
                    boolean success = indexPage.tryInsert(indexBytes);
                    if(!success){
                        indexPageFile.writePage(indexPage , pageNo);
                        pageNo++;
                        indexPage = IndexPage.createIndexPage();
                        success = indexPage.tryInsert(indexBytes);
                        if(!success){
                            throw new IndexException("Index is too large for a single page");
                        }
                    }
                }
            }
            indexPageFile.writePage(indexPage , pageNo);
        }catch(IOException e){
            throw new IndexException("Could not create index", e);
        }
    }

    public Map<Value , LinkedHashSet<RecordId>> readIndex(IndexMetaData metaData) throws IOException {
        Path indexPath = resolveIndexPath(metaData);
        Map<Value , LinkedHashSet<RecordId>> indexMap = new LinkedHashMap<>();
        Type colType = metaData.colType();
        try{
            IndexPageFile indexPageFile = new IndexPageFile(indexPath);
            int pageCount = indexPageFile.getPageCount();
            for(int i = 0 ; i < pageCount ; i++){
                IndexPage page = indexPageFile.readPage(i);
                List<byte[]> indexBytes = page.getIndexes();
                for(byte[] indexByte : indexBytes){
                    IndexValueRecordId vrid = binaryIndexSerializer.deserialize(indexByte , colType);
                    Value value = vrid.value();
                    RecordId recordId = vrid.recordId();
                    indexMap.computeIfAbsent(value, k -> new LinkedHashSet<>()).add(recordId);
                }
            }
            return indexMap;
        }catch(IOException e){
            throw new IndexException("Could not read index", e);
        }

    }




    //RESOLVING PATHS//
    private Path resolveIndexPath(IndexMetaData indexMetaData){
        return resolveIndexesDir(indexMetaData.tableName()).resolve(indexMetaData.fileName());
    }

    private Path resolveTableDir(String tableName){
        validateIdentifier(tableName , "Table");
        return dbRoot.resolve(tableName);
    }
    private Path resolveIndexesDir(String tableName){
        return resolveTableDir(tableName).resolve(INDEX_DIR_NAME);
    }
    private Path resolveCatalogPath(String tableName){
        return resolveIndexesDir(tableName).resolve(INDEXES_META_NAME);
    }

    private void validateIdentifier(String identifier, String fieldName){
        if(identifier == null || identifier.isEmpty()){
            throw new IndexException("Identifier is null or empty");
        }

        if (!identifier.matches("[a-zA-Z_][a-zA-Z0-9_]*")) {
            throw new IndexException("Invalid " + fieldName + ": " + identifier);
        }
    }
}
