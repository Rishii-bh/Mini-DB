package legacy_tests_disables;

import MiniDB.Index.IndexManager;
import MiniDB.StorageEngine.PageFileStorage;
import MiniDB.StorageEngine.RecordId;
import MiniDB.StorageEngine.RowWithRecordId;
import MiniDB.core.Row;
import MiniDB.query.QueryEngine;
import MiniDB.query.QueryResult;
import MiniDB.query.results.SelectQueryResult;
import MiniDB.sql.SqlRunner;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.nio.file.Path;
import java.util.List;
import java.util.Optional;

import static org.junit.jupiter.api.Assertions.*;

public class SlotReuseTests {
    @TempDir
    Path tempDir;

    @Test
    void deletedSlotsShouldBeReused() {
        PageFileStorage pageFileStorage = new PageFileStorage(tempDir);
        IndexManager indexManager = new IndexManager(tempDir,pageFileStorage);
        QueryEngine queryEngine = new QueryEngine(pageFileStorage, indexManager);
        SqlRunner runner = new SqlRunner(queryEngine);

        runner.execute("CREATE TABLE students (id INT, name TEXT, active BOOL);");
        runner.execute("Insert into students (id, name, active) values (1, \"Rishi\", true);");

        RowWithRecordId oldRowRef = pageFileStorage.scanRows("students").getFirst();
        RecordId oldRid = oldRowRef.recordId();

        runner.execute("DELETE FROM students WHERE id = 1;");

        runner.execute("INSERT INTO students (id, name, active) VALUES (2, \"Sara\", false);");

        List<RowWithRecordId> rows = pageFileStorage.scanRows("students");

        Assertions.assertEquals(1, rows.size());

        RowWithRecordId newRowRef = rows.getFirst();

        Assertions.assertEquals(oldRid, newRowRef.recordId());
        Assertions.assertEquals(2, newRowRef.row().getValue(0));
        Assertions.assertEquals("Sara", newRowRef.row().getValue(1));

    }


    @Test
    void reUsedRecordIdDirectReadReturnsNewRow(){
        PageFileStorage pageFileStorage = new PageFileStorage(tempDir);
        IndexManager indexManager = new IndexManager(tempDir,pageFileStorage);
        QueryEngine queryEngine = new QueryEngine(pageFileStorage, indexManager);
        SqlRunner runner = new SqlRunner(queryEngine);

        runner.execute("CREATE TABLE students (id INT, name TEXT, active BOOL);");
        runner.execute("Insert into students (id, name, active) values (1, \"Rishi\", true);");

        RowWithRecordId oldRowRef = pageFileStorage.scanRows("students").getFirst();
        RecordId oldRid = oldRowRef.recordId();

        runner.execute("DELETE FROM students WHERE id = 1;");

        runner.execute("INSERT INTO students (id, name, active) VALUES (2, \"Sara\", false);");

        Optional<Row> row = pageFileStorage.getRowByRecordId("students", oldRid);
        Assertions.assertTrue(row.isPresent());
        Assertions.assertEquals(2,row.get().getValue(0));
        Assertions.assertEquals("Sara",row.get().getValue(1));

    }

    @Test
    void insertDoesNotReuseDeletedSlotWhenNewRowIsTooLarge() throws Exception {
        PageFileStorage storage = new PageFileStorage(tempDir);
        IndexManager indexManager = new IndexManager(tempDir,storage);
        QueryEngine engine = new QueryEngine(storage, indexManager);
        SqlRunner runner = new SqlRunner(engine);

        runner.execute("CREATE TABLE students (id INT, name TEXT, active BOOL);");

        runner.execute("INSERT INTO students (id, name, active) VALUES (1, \"A\", true);");

        RecordId smallRid = storage.scanRows("students").getFirst().recordId();

        runner.execute("DELETE FROM students WHERE id = 1;");

        runner.execute("INSERT INTO students (id, name, active) VALUES (2, \"ThisNameIsMuchLongerThanA\", false);");

        List<RowWithRecordId> rows = storage.scanRows("students");

        Assertions.assertEquals(1, rows.size());

        RecordId newRid = rows.getFirst().recordId();

        Assertions.assertNotEquals(smallRid, newRid);
    }

    @Test
    void selectFindsRowInsertedIntoReusedSlot() throws Exception {
        PageFileStorage storage = new PageFileStorage(tempDir);
        IndexManager indexManager = new IndexManager(tempDir,storage);
        QueryEngine engine = new QueryEngine(storage, indexManager);
        SqlRunner runner = new SqlRunner(engine);

        runner.execute("CREATE TABLE students (id INT, name TEXT, active BOOL);");

        runner.execute("INSERT INTO students (id, name, active) VALUES (1, \"Rishi\", true);");
        runner.execute("DELETE FROM students WHERE id = 1;");
        runner.execute("INSERT INTO students (id, name, active) VALUES (2, \"Sara\", false);");

        QueryResult result = runner.execute("SELECT name FROM students WHERE id = 2;");

        Assertions.assertInstanceOf(SelectQueryResult.class, result);

        SelectQueryResult select = (SelectQueryResult) result;

        Assertions.assertEquals(1, select.getRowCount());
        Assertions.assertEquals("Sara", select.getResultRows().getFirst().getValue(0));
    }
}
