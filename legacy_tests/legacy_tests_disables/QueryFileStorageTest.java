package legacy_tests_disables;

import MiniDB.Index.IndexManager;
import MiniDB.StorageEngine.*;
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
        PageFileStorage pageFileStorage = new PageFileStorage(tempDir);
        IndexManager indexManager = new IndexManager(pageFileStorage);
        QueryEngine queryEngine = new QueryEngine(pageFileStorage , indexManager);
        SqlRunner sqlRunner = new SqlRunner(queryEngine);

        sqlRunner.execute("CREATE TABLE students (id INT, name TEXT, active BOOL);");
        sqlRunner.execute("INSERT INTO students (id, name, active) VALUES (1, \"Rishi\", true);");

        QueryResult result = sqlRunner.execute(
                "SELECT id, name, active FROM students WHERE id = 1;"
        );

        Assertions.assertTrue(result instanceof SelectQueryResult);

        SelectQueryResult resultSet = (SelectQueryResult) result;

        Assertions.assertEquals(1, resultSet.getRowCount());
        Assertions.assertEquals(1, resultSet.getRow(0).getValue(0));
        Assertions.assertEquals("Rishi", resultSet.getRow(0).getValue(1));
        Assertions.assertEquals(true, resultSet.getRow(0).getValue(2));

    }
}
