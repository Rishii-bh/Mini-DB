package IntegrationTests;

import MiniDB.DatabaseRunner.DatabaseRunner;
import MiniDB.query.QueryResult;
import MiniDB.query.results.SelectQueryResult;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.nio.file.Path;

import static org.junit.jupiter.api.Assertions.assertInstanceOf;
import static org.junit.jupiter.api.Assertions.assertTrue;

public class QueryTests {
    @TempDir
    Path tempDir;


    public DatabaseRunner getDbRunner() {
        return new DatabaseRunner(tempDir);
    }

    @Test
    void createTableInsertIntoTableAndSelectFromTable() {
       DatabaseRunner dbRunner = getDbRunner();

        dbRunner.execute("CREATE TABLE students (id INT, name TEXT, active BOOL);");
        dbRunner.execute("INSERT INTO students (id, name, active) VALUES (1, \"Rishi\", true);");

        QueryResult result = dbRunner.execute(
                "SELECT id, name, active FROM students WHERE id = 1;"
        );

        assertInstanceOf(SelectQueryResult.class, result);

        SelectQueryResult resultSet = (SelectQueryResult) result;

        Assertions.assertEquals(1, resultSet.getRowCount());
        Assertions.assertEquals(1, resultSet.getRow(0).getValue(0));
        Assertions.assertEquals("Rishi", resultSet.getRow(0).getValue(1));
        Assertions.assertEquals(true, resultSet.getRow(0).getValue(2));

    }
}
