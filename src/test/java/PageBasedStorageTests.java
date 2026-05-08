import MiniDB.StorageEngine.*;
import MiniDB.core.Row;
import MiniDB.core.Schema;
import MiniDB.query.QueryEngine;
import MiniDB.query.QueryResult;
import MiniDB.query.results.SelectQueryResult;
import MiniDB.sql.SqlRunner;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.nio.file.Path;
import java.util.ArrayList;
import java.util.List;

import static org.junit.jupiter.api.Assertions.*;

public class PageBasedStorageTests {
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
    void insertManyRowsSpansMultiplePages() throws Exception {
        SqlRunner runner = newRunner();
        runner.execute("CREATE TABLE students (id INT, name TEXT, active BOOL);");

        String longName = "x".repeat(1000);

        for (int i = 0; i < 20; i++) {
            runner.execute("INSERT INTO students (id, name, active) VALUES (" + i + ", \"" + longName + "\", true);");
        }

        QueryResult result = runner.execute("SELECT id, name, active FROM students WHERE active = true;");
        assertInstanceOf(SelectQueryResult.class, result);
        SelectQueryResult selectQueryResult = (SelectQueryResult) result;
        assertEquals(20, selectQueryResult.getRowCount());
    }

    @Test
    void checkRowSurvivesRestart() throws Exception {
        SqlRunner runner = newRunner();
        runner.execute("CREATE TABLE students (id INT, name TEXT, active BOOL);");
        runner.execute("Insert into students (id, name, active) values (1, \"Rishi\", true);");
        SqlRunner runner2 = newRunner();
        QueryResult result = runner2.execute("SELECT id, name, active FROM students WHERE active = true;");
        assertInstanceOf(SelectQueryResult.class, result);
        SelectQueryResult selectQueryResult = (SelectQueryResult) result;
        assertEquals(1, selectQueryResult.getRowCount());
    }

    @Test
    void deleteRowsShouldReplaceDeletedRows() throws Exception {
        SqlRunner runner = newRunner();
        runner.execute("CREATE TABLE students (id INT, name TEXT, active BOOL);");
        String name = "Rishi";
        String name2 = "Alex";
        for(int i=0; i<10; i++) {
            if(i%2==0) {
                runner.execute("INSERT INTO students (id, name, active) VALUES (" + i + ", \"" + name+ "\", true);");
            }
            else {
                runner.execute("INSERT INTO students (id, name, active) VALUES (" + i + ", \"" + name2 + "\", false);");
            }
        }
        runner.execute("DELETE FROM students WHERE name = \"Rishi\";");
        QueryResult result = runner.execute("SELECT id, name, active FROM students WHERE name = \"Alex\";");
        assertInstanceOf(SelectQueryResult.class, result);
        SelectQueryResult selectQueryResult = (SelectQueryResult) result;
        assertEquals(5, selectQueryResult.getRowCount());
    }
}
