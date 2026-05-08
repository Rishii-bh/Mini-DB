import MiniDB.StorageEngine.*;
import MiniDB.core.Column;
import MiniDB.core.Row;
import MiniDB.core.Schema;
import MiniDB.core.Type;
import MiniDB.query.QueryEngine;
import MiniDB.sql.SqlRunner;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.io.DataInputStream;
import java.io.EOFException;
import java.io.IOException;
import java.io.InputStream;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.List;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;

public class BadQueryTests {

    @TempDir
    Path tempDir;

    private SqlRunner newRunner() {
        PageFileStorage pageFileStorage = new PageFileStorage(tempDir);
        QueryEngine queryEngine = new QueryEngine(pageFileStorage);
        return new SqlRunner(queryEngine);
    }

    private List<Row> readBinaryRows(Path rowsPath, Schema schema) {
        BinaryRowSerializer binaryRowSerializer = new BinaryRowSerializer();
        PageFile pageFile = new PageFile(rowsPath);
        List<Row> rows = new ArrayList<>();
        try{
            int numOfPages = pageFile.getPageCount();
            for (int i = 0; i < numOfPages; i++) {
                Page page = pageFile.readPage(i);
                int slotCount = page.getSlotCount();
                for (int j = 0; j < slotCount; j++) {
                    byte[] rowBytes = page.getRowBytes(j);
                    Row row = binaryRowSerializer.deserialize(rowBytes, schema);
                    rows.add(row);
                }
            }


        } catch (Exception e) {
            throw new StorageException("Could not read Page File", e);
        }
        return rows;
    }

    @Test
    void insertWithWrongTypeThrowsAndDoesNotMutateRowsFile() throws Exception {
        SqlRunner runner = newRunner();

        runner.execute("CREATE TABLE students (id INT, name TEXT, active BOOL);");
        runner.execute("INSERT INTO students (id, name, active) VALUES (1, \"Rishi\", true);");

        Path rowsPath = tempDir.resolve("students").resolve("table.dat");
        Schema schema = new Schema(List.of(
                new Column("id", Type.INT),
                new Column("name", Type.TEXT),
                new Column("active", Type.BOOL)
        ));
        List<Row> before = readBinaryRows(rowsPath, schema);

        assertThrows(RuntimeException.class, () ->
                runner.execute("INSERT INTO students (id, name, active) VALUES (\"wrong\", \"Sara\", false);")
        );
        List<Row> after = readBinaryRows(rowsPath, schema);
        assertEquals(before.size(), after.size());
        assertEquals(before.getFirst().getRow(), after.getFirst().getRow());
    }
    @Test
    void insertWithUnknownColumnThrowsAndDoesNotMutateRowsFile() throws Exception {
        SqlRunner runner = newRunner();

        runner.execute("CREATE TABLE students (id INT, name TEXT);");
        runner.execute("INSERT INTO students (id, name) VALUES (1, \"Rishi\");");

        Path rowsPath = tempDir.resolve("students").resolve("table.dat");
        Schema schema = new Schema(List.of(
                new Column("id", Type.INT),
                new Column("name", Type.TEXT)
        ));
        List<Row> before = readBinaryRows(rowsPath, schema);


        assertThrows(RuntimeException.class, () ->
                runner.execute("INSERT INTO students (id, age) VALUES (2, 20);")
        );
        List<Row> after = readBinaryRows(rowsPath, schema);
        assertEquals(before.size(), after.size());
        assertEquals(before.getFirst().getRow(), after.getFirst().getRow());
    }

    @Test
    void deleteWithWrongConditionTypeThrowsAndDoesNotMutateRowsFile() throws Exception {
        SqlRunner runner = newRunner();

        runner.execute("CREATE TABLE students (id INT, name TEXT);");
        runner.execute("INSERT INTO students (id, name) VALUES (1, \"Rishi\");");

        Path rowsPath = tempDir.resolve("students").resolve("table.dat");
        Schema schema = new Schema(List.of(
                new Column("id", Type.INT),
                new Column("name", Type.TEXT)
        ));
        List<Row> before = readBinaryRows(rowsPath, schema);


        assertThrows(RuntimeException.class, () ->
                runner.execute("DELETE FROM students WHERE id = \"wrong\";")
        );

        List<Row> after = readBinaryRows(rowsPath, schema);
        assertEquals(before.size(), after.size());
        assertEquals(before.getFirst().getRow(), after.getFirst().getRow());
    }
}
