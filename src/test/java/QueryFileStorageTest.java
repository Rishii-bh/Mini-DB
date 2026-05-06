import MiniDB.StorageEngine.BinaryFileStorage;
import MiniDB.StorageEngine.TextFileStorageEngine;
import MiniDB.StorageEngine.RowSerializer;
import MiniDB.StorageEngine.SchemaSerializer;
import MiniDB.query.QueryEngine;
import MiniDB.query.QueryResult;
import MiniDB.query.results.SelectQueryResult;
import MiniDB.sql.SqlRunner;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.io.IOException;
import java.nio.file.Path;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

public class QueryFileStorageTest {
    @TempDir
    Path tempDir;

    @Test
    void createTableInsertIntoTableAndSelectFromTable() throws IOException {
        BinaryFileStorage binaryFileStorage = new BinaryFileStorage(tempDir);
        QueryEngine queryEngine = new QueryEngine(binaryFileStorage);
        SqlRunner sqlRunner = new SqlRunner(queryEngine);

        sqlRunner.execute("CREATE TABLE students (id INT, name TEXT, active BOOL);");
        sqlRunner.execute("INSERT INTO students (id, name, active) VALUES (1, \"Rishi\", true);");

        QueryResult result = sqlRunner.execute(
                "SELECT id, name, active FROM students WHERE id = 1;"
        );

        assertTrue(result instanceof SelectQueryResult);

        SelectQueryResult resultSet = (SelectQueryResult) result;

        assertEquals(1, resultSet.getRowCount());
        assertEquals(1, resultSet.getRow(0).getValue(0));
        assertEquals("Rishi", resultSet.getRow(0).getValue(1));
        assertEquals(true, resultSet.getRow(0).getValue(2));

    }
}
