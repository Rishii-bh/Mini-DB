import MiniDB.StorageEngine.IndexManager;
import MiniDB.StorageEngine.PageFileStorage;
import MiniDB.StorageEngine.RecordId;
import MiniDB.StorageEngine.RowWithRecordId;
import MiniDB.core.Type;
import MiniDB.core.Value;
import MiniDB.core.ValueFactory;
import MiniDB.query.QueryEngine;
import MiniDB.sql.SqlRunner;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.nio.file.Path;
import java.util.LinkedHashSet;
import java.util.List;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;

public class IndexTests {
    @TempDir
    Path tempDir;
//this test proves that a delete Query on a non indexed Column deletes all existence of the recordId in
    //specific buckets .
    @Test
    void deleteQueryOnANonIndexedColumn(){
        PageFileStorage pageFileStorage = new PageFileStorage(tempDir);
        IndexManager indexManager = new IndexManager(pageFileStorage);
        QueryEngine queryEngine = new QueryEngine(pageFileStorage, indexManager);
        SqlRunner runner = new SqlRunner(queryEngine);

        runner.execute("CREATE TABLE students (id INT, name TEXT, active BOOL);");
        runner.execute("Insert into students (id, name, active) values (1, \"Rishi\", true);");
        runner.execute("Insert into students (id, name, active) values (2, \"Sara\", true);");
        runner.execute("Insert into students (id, name, active) values (1, \"Alex\", false);");
        runner.execute("Insert into students (id, name, active) values (2, \"Brit\", false);");

        indexManager.createIndex("students", "id");
        List<RowWithRecordId> originalRowsWithRecordId = pageFileStorage.scanRows("students");
        RecordId rishiRecordId = originalRowsWithRecordId.getFirst().getRecordId();
        RecordId saraRecordId = originalRowsWithRecordId.get(1).getRecordId();

        runner.execute("Delete from students where active = false;");
        Value valueForId1 = ValueFactory.fromLiteral(1, Type.INT);
        Value valueForId2 = ValueFactory.fromLiteral(2, Type.INT);


        LinkedHashSet<RecordId> bucket1 = indexManager.search("students","id",valueForId1);
        LinkedHashSet<RecordId> bucket2 = indexManager.search("students","id",valueForId2);
        assertEquals(1, bucket1.size());
        assertEquals(1, bucket2.size());
        assertEquals(rishiRecordId, bucket1.getFirst());
        assertEquals(saraRecordId, bucket2.getFirst());
    }

    //this test aims to prove that a record should be deleted from multiple indexes inside a table
    @Test
    void deleteQueryRemovesIndicesAcrossMultipleIndexMaps(){
        PageFileStorage pageFileStorage = new PageFileStorage(tempDir);
        IndexManager indexManager = new IndexManager(pageFileStorage);
        QueryEngine queryEngine = new QueryEngine(pageFileStorage, indexManager);
        SqlRunner runner = new SqlRunner(queryEngine);

        runner.execute("CREATE TABLE students (id INT, name TEXT, active BOOL);");
        runner.execute("Insert into students (id, name, active) values (1, \"Rishi\", true);");
        runner.execute("Insert into students (id, name, active) values (2, \"Sara\", true);");
        runner.execute("Insert into students (id, name, active) values (1, \"Alex\", false);");

        indexManager.createIndex("students", "id");
        indexManager.createIndex("students", "name");

        List<RowWithRecordId> rowsWithRecordId = pageFileStorage.scanRows("students");
        RecordId rishiRecordId = rowsWithRecordId.getFirst().getRecordId();

        runner.execute("Delete from students where name = \"Rishi\";");

        Value valueForId1 = ValueFactory.fromLiteral(1, Type.INT);
        Value valueForRishi = ValueFactory.fromLiteral("Rishi", Type.TEXT);

        LinkedHashSet<RecordId> idBucket = indexManager.search("students","id",valueForId1);
        LinkedHashSet<RecordId> nameBucket = indexManager.search("students","name",valueForRishi);
        assertFalse(idBucket.contains(rishiRecordId));
        assertFalse(nameBucket.contains(rishiRecordId));
    }



}
