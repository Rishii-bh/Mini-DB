package NonHappyPathTests;

import MiniDB.DatabaseRunner.DatabaseRunner;
import MiniDB.StorageEngine.PageFileStorage;
import MiniDB.StorageEngine.RowWithRecordId;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.nio.file.Path;
import java.util.List;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;

public class BadQueryTests {
    @TempDir
    Path tempDir;

    public DatabaseRunner getDbRunner() {
        return new DatabaseRunner(tempDir);
    }

    @Test
    void insertWithWrongTypeThrowsAndDoesNotMutateRowsFile() throws Exception{
        DatabaseRunner dbRunner = getDbRunner();
        dbRunner.start();
        dbRunner.execute("CREATE TABLE students (id INT, name TEXT, active BOOL);");
        dbRunner.execute("INSERT INTO students (id, name, active) VALUES (1, \"Rishi\", true);");
        dbRunner.execute("INSERT INTO students (id, name, active) VALUES (2, \"Tobi\", true);");

        PageFileStorage storage = dbRunner.getStorage();
        List<RowWithRecordId> originalRows = storage.scanRows("students");

        assertThrows(RuntimeException.class, () -> dbRunner.execute("INSERT INTO students (id, name, active) VALUES (\"wrong\", \"Rishi\", trash);"));

        dbRunner.shutdown();

        DatabaseRunner newRunner = getDbRunner();
        newRunner.start();
        PageFileStorage newStorage = newRunner.getStorage();
        List<RowWithRecordId> newRows = newStorage.scanRows("students");

        assertEquals(originalRows.size(), newRows.size());
        assertEquals(originalRows.getFirst().row().getValue(0), newRows.getFirst().row().getValue(0));
        assertEquals(originalRows.getFirst().row().getValue(1), newRows.getFirst().row().getValue(1));
    }


    @Test
    void deleteWithWrongTypeThrowsAndDoesNotMutateRowsFile() throws Exception{
        DatabaseRunner dbRunner = getDbRunner();
        dbRunner.start();
        dbRunner.execute("CREATE TABLE students (id INT, name TEXT, active BOOL);");
        dbRunner.execute("INSERT INTO students (id, name, active) VALUES (1, \"Rishi\", true);");
        dbRunner.execute("INSERT INTO students (id, name, active) VALUES (2, \"Tobi\", true);");

        PageFileStorage storage = dbRunner.getStorage();
        List<RowWithRecordId> originalRows = storage.scanRows("students");

        assertThrows(RuntimeException.class, () -> dbRunner.execute("Delete abcd from students where id = 1;"));

        dbRunner.shutdown();

        DatabaseRunner newRunner = getDbRunner();
        newRunner.start();
        PageFileStorage newStorage = newRunner.getStorage();
        List<RowWithRecordId> newRows = newStorage.scanRows("students");

        assertEquals(originalRows.size(), newRows.size());
        assertEquals(originalRows.getFirst().row().getValue(0), newRows.getFirst().row().getValue(0));
        assertEquals(originalRows.getFirst().row().getValue(1), newRows.getFirst().row().getValue(1));
    }


}
