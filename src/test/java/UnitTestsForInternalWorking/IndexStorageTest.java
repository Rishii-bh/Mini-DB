package UnitTestsForInternalWorking;

import MiniDB.Index.*;
import MiniDB.StorageEngine.RecordId;
import MiniDB.core.Type;
import MiniDB.core.Value;
import MiniDB.core.ValueFactory;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.io.IOException;
import java.nio.file.Path;
import java.util.LinkedHashMap;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.assertDoesNotThrow;
import static org.junit.jupiter.api.Assertions.assertEquals;

public class IndexStorageTest {
    @TempDir
    Path tempDir;

    @Test
    void storeIndexesAndRetrieveThem() throws IOException {
        IndexMetaData metaData = createMetadata("students","name",Type.TEXT);
        Value value1 = ValueFactory.fromLiteral("Rishi", Type.TEXT);
        Value value2 = ValueFactory.fromLiteral("Tobi", Type.TEXT);
        RecordId rid1 = new RecordId(0,0);
        RecordId rid2 = new RecordId(1,1);
        Map<Value , LinkedHashSet<RecordId>> indexMap = new LinkedHashMap<>();
        indexMap.computeIfAbsent(value1, k -> new LinkedHashSet<>()).add(rid1);
        indexMap.computeIfAbsent(value2, k -> new LinkedHashSet<>()).add(rid2);
        IndexStorageEngine engine = createStorageEngine(new IndexCatalog());
        assertDoesNotThrow(() ->engine.createIndex(metaData,indexMap));
        Map<Value , LinkedHashSet<RecordId>> storedIndex = assertDoesNotThrow(()-> engine.readIndex(metaData));
        assertEquals(indexMap.size(), storedIndex.size());
        assertEquals(storedIndex.get(value1).getFirst() , rid1);
        assertEquals(storedIndex.get(value2).getFirst() , rid2);

    }

    @Test
    void indexStoreAndRetrieveCatalog() throws IOException {
        String tableName = "students";
        IndexMetaData metaData1 = createMetadata(tableName,"id",Type.INT);
        IndexMetaData metaData2 = createMetadata(tableName,"name",Type.TEXT);
        IndexStorageEngine engine = createStorageEngine(new IndexCatalog());
        engine.storeCatalog(tableName,metaData1);
        engine.storeCatalog(tableName,metaData2);
        List<IndexMetaData> storedMetadata = engine.loadCatalog(tableName);
        assertEquals(2, storedMetadata.size());
        assertEquals(metaData1, storedMetadata.get(0));
        assertEquals(metaData2, storedMetadata.get(1));

    }

    //helper
    public IndexMetaData createMetadata(String tableName, String columnName, Type columnType) {
        IndexType indexType = IndexType.FLAT;
        String fileName = tempDir.resolve(tableName).resolve(columnName).resolve(".idx").toString();
        return new IndexMetaData(tableName, columnName, columnType, indexType, fileName);
    }

    public IndexStorageEngine createStorageEngine(IndexCatalog catalog) {
        return new IndexStorageEngine(tempDir , catalog , new BinaryIndexSerializer());
    }

}
