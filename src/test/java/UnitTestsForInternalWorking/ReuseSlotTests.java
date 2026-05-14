package UnitTestsForInternalWorking;

import MiniDB.DatabaseRunner.DatabaseRunner;
import MiniDB.StorageEngine.PageFileStorage;
import MiniDB.StorageEngine.RecordId;
import MiniDB.StorageEngine.RowWithRecordId;
import MiniDB.core.Row;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.nio.file.Path;
import java.util.List;
import java.util.Optional;

import static org.junit.jupiter.api.Assertions.*;

public class ReuseSlotTests {

    @TempDir
    Path tempDir;

    public DatabaseRunner getDatabaseRunner() {
        return new DatabaseRunner(tempDir);
    }

    @Test
    void checkIfInsertionAfterDeletionReusesSlot() throws Exception {
        //setup
        DatabaseRunner dbRunner = getDatabaseRunner();
        dbRunner.start();
        dbRunner.execute("CREATE TABLE students (id INT, name TEXT, active BOOL);");
        dbRunner.execute("INSERT INTO students (id, name, active) VALUES (1, \"Rishi\", true);");
        dbRunner.execute("INSERT INTO students (id, name, active) VALUES (2, \"Tobi\", false);");
//we need to check storage behaviour

        PageFileStorage storage = dbRunner.getStorage();
        List<RowWithRecordId> rowWithRecordIds = storage.scanRows("students");
        RecordId originalRecordId = rowWithRecordIds.getFirst().getRecordId();
        Row originalRow = rowWithRecordIds.getFirst().row();

        //now delete Should Mark Slot deleted. next insert should delete exactly in that slot if it fits
        dbRunner.execute("DELETE FROM students WHERE id = 1;");

        dbRunner.execute("INSERT INTO students (id, name, active) VALUES (3, \"AB\", true);");

        //now we check to see if our new row was inserted in the place of the deleted Row
        Optional<Row> row = storage.getRowByRecordId("students",originalRecordId);
        assertTrue(row.isPresent());
        Row updatedRow = row.get();
        assertEquals(3 , updatedRow.getValue(0));
        assertEquals("AB", updatedRow.getValue(1));
        assertEquals(true , updatedRow.getValue(2));
    }

    @Test
    void checkIfInsertionOfLargerRowAfterDeletionDoesntOverflow() throws Exception {
        //setup
        DatabaseRunner dbRunner = getDatabaseRunner();
        dbRunner.start();
        dbRunner.execute("CREATE TABLE students (id INT, name TEXT, active BOOL);");
        dbRunner.execute("INSERT INTO students (id, name, active) VALUES (1, \"Rishi\", true);");
//we need to check storage behaviour

        PageFileStorage storage = dbRunner.getStorage();
        List<RowWithRecordId> rowWithRecordIds = storage.scanRows("students");
        RecordId originalRecordId = rowWithRecordIds.getFirst().getRecordId();

        //now delete Should Mark Slot deleted. next insert should delete exactly in that slot if it fits
        dbRunner.execute("DELETE FROM students WHERE id = 1;");

        dbRunner.execute("INSERT INTO students (id, name, active) VALUES (2, \"Alexandra\", true);");

        //now we check to see if our new row was not inserted in the place of the deleted Row
       List<RowWithRecordId> newRowsWithRecordIds = storage.scanRows("students");
       Row newRow = newRowsWithRecordIds.getFirst().row();
       RecordId newRowWithRecordId = newRowsWithRecordIds.getFirst().recordId();
       assertNotEquals(originalRecordId, newRowWithRecordId);
       assertEquals(2,newRow.getValue(0));
       assertEquals("Alexandra",newRow.getValue(1));
       assertEquals(true ,newRow.getValue(2));
    }


}
