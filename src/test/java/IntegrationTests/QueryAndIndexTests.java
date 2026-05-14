package IntegrationTests;

import MiniDB.DatabaseRunner.DatabaseRunner;
import MiniDB.Index.IndexManager;
import MiniDB.Index.SpyIndexManager;
import MiniDB.StorageEngine.PageFileStorage;
import MiniDB.StorageEngine.RecordId;
import MiniDB.StorageEngine.RowWithRecordId;
import MiniDB.StorageEngine.SchemaManager;
import MiniDB.core.Database;
import MiniDB.core.Type;
import MiniDB.core.Value;
import MiniDB.core.ValueFactory;
import MiniDB.query.QueryResult;
import MiniDB.query.rawqueries.SelectQuery;
import MiniDB.query.results.SelectQueryResult;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.nio.file.Path;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.stream.Collectors;

import static org.junit.jupiter.api.Assertions.*;

public class QueryAndIndexTests {
    @TempDir
    Path tempDir;


    public DatabaseRunner getDbRunner() {
        return new DatabaseRunner(tempDir);
    }

    public DatabaseRunner getSpyDbRunner() {
        SchemaManager schemaManager = new SchemaManager(tempDir);
        PageFileStorage pageFileStorage = new PageFileStorage(schemaManager,tempDir);
        SpyIndexManager spyIndexManager = new SpyIndexManager(tempDir,pageFileStorage,schemaManager);
        return new DatabaseRunner(tempDir,schemaManager,pageFileStorage,spyIndexManager);
    }
    @Test
    void testCreateIndexPersistsAfterReload() throws Exception {
        DatabaseRunner dbRunner = getDbRunner();
        dbRunner.start();
        dbRunner.execute("CREATE TABLE students (id INT, name TEXT, active BOOL);");
        dbRunner.execute("INSERT INTO students (id, name, active) VALUES (1, \"Rishi\", true);");
        dbRunner.execute("INSERT INTO students (id, name, active) VALUES (2, \"Tobi\", true);");
        String tableName = "students";
        String columnName = "id";
        IndexManager indexManager = dbRunner.getIndexManager();
        assertDoesNotThrow(() ->indexManager.createIndex(tableName, columnName));
        dbRunner.shutdown();
        DatabaseRunner dbRunner2 = getDbRunner();
        dbRunner2.start();
        IndexManager indexManager2 = dbRunner2.getIndexManager();
        assertTrue(indexManager2.indexExists(tableName, columnName));
    }

    @Test
    void postIndexInsertIsFoundAfterRestart() throws Exception {
        DatabaseRunner dbRunner = getDbRunner();
        dbRunner.start();
        dbRunner.execute("CREATE TABLE students (id INT, name TEXT, active BOOL);");
        dbRunner.execute("INSERT INTO students (id, name, active) VALUES (1, \"Rishi\", true);");
        dbRunner.execute("INSERT INTO students (id, name, active) VALUES (2, \"Tobi\", true);");
        String tableName = "students";
        String columnName = "id";
        IndexManager indexManager = dbRunner.getIndexManager();
        assertDoesNotThrow(() ->indexManager.createIndex(tableName, columnName));
        dbRunner.execute("INSERT INTO students (id, name, active) VALUES (1, \"Rishi\", false);");
        dbRunner.shutdown();

        DatabaseRunner dbRunner2 = getDbRunner();
        dbRunner2.start();
        IndexManager indexManager2 = dbRunner2.getIndexManager();
        assertTrue(indexManager2.indexExists(tableName, columnName));
        Value key = ValueFactory.fromLiteral(1, Type.INT);
        LinkedHashSet<RecordId> recordIds = assertDoesNotThrow(()->indexManager2.search(tableName, columnName, key));
        assertEquals(2, recordIds.size());
    }

    @Test
    void selectQueryUsesStoredIndexForLookup() throws Exception {
        //creating database
        DatabaseRunner dbRunner = getDbRunner();
        dbRunner.start();
        dbRunner.execute("CREATE TABLE students (id INT, name TEXT, active BOOL);");
        dbRunner.execute("INSERT INTO students (id, name, active) VALUES (1, \"Rishi\", true);");
        dbRunner.execute("INSERT INTO students (id, name, active) VALUES (2, \"Tobi\", true);");
        String tableName = "students";
        String columnName = "id";
        IndexManager indexManager = dbRunner.getIndexManager();
        assertDoesNotThrow(() ->indexManager.createIndex(tableName, columnName));
        dbRunner.execute("INSERT INTO students (id, name, active) VALUES (1, \"Rishi\", false);");
        dbRunner.shutdown();
        //running database with mock managers to check for behaviour
       DatabaseRunner dbRunner2 = getSpyDbRunner();
       dbRunner2.start();
       SpyIndexManager spyIndexManager = (SpyIndexManager) dbRunner2.getIndexManager();

        //checking if cache is loaded
        assertTrue(spyIndexManager.indexExists(tableName, columnName));

        //select query
        QueryResult result = dbRunner2.execute("SELECT name FROM students WHERE id = 1;");

        //check if select query really uses the index
        assertTrue(spyIndexManager.searchCalled);

        //select query results return expected value
        assertInstanceOf(SelectQueryResult.class, result);
        SelectQueryResult selectQueryResult = (SelectQueryResult) result;
        assertEquals(2,selectQueryResult.getRowCount());

        List<Object> names = selectQueryResult.getResultRows().stream().map(row -> row.getValue(0)).toList();
        long nameCount = names.stream().filter(name -> name.equals("Rishi")).count();
        assertEquals(2,nameCount);
    }

    // this next test is for deleteQuery
    @Test
    void deletedRowsAreNotReturnedByIndexedSelectAfterRestart() throws Exception {
        //setup
        DatabaseRunner dbRunner = getDbRunner();
        dbRunner.start();
        dbRunner.execute("CREATE TABLE students (id INT, name TEXT, active BOOL);");
        dbRunner.execute("INSERT INTO students (id, name, active) VALUES (1, \"Rishi\", true);");
        dbRunner.execute("INSERT INTO students (id, name, active) VALUES (2, \"Tobi\", true);");
        dbRunner.execute("INSERT INTO students (id, name, active) VALUES (1, \"Rishi\", false);");
        dbRunner.execute("INSERT INTO students (id, name, active) VALUES (2, \"Tobi\", false);");

        String tableName = "students";
        String columnName = "id";
        IndexManager indexManager = dbRunner.getIndexManager();
        assertDoesNotThrow(() ->indexManager.createIndex(tableName, columnName));
        dbRunner.execute("DELETE FROM students WHERE active = false;");
        dbRunner.shutdown();

        //setup with spy indexManager to test if select is using indexes and if indexes have truly
        //been updated
        DatabaseRunner dbRunner2 = getSpyDbRunner();
        dbRunner2.start();
        SpyIndexManager spyIndexManager = (SpyIndexManager) dbRunner2.getIndexManager();

        //checking if cache is loaded
        assertTrue(spyIndexManager.indexExists(tableName, columnName));

        //select query
        QueryResult result = dbRunner2.execute("SELECT name FROM students WHERE id =1;");
        QueryResult result2 = dbRunner2.execute("SELECT name FROM students WHERE id =2;");

        //check if select query really uses the index
        assertTrue(spyIndexManager.searchCalled);

        //check if select query returns the right result
        assertInstanceOf(SelectQueryResult.class, result);
        assertInstanceOf(SelectQueryResult.class, result2);
        SelectQueryResult selectQueryResult = (SelectQueryResult) result;
        SelectQueryResult selectQueryResult2 = (SelectQueryResult) result2;
        assertEquals(1, selectQueryResult.getRowCount());
        assertEquals(1 ,selectQueryResult2.getRowCount());

        List<Object> names = selectQueryResult.getResultRows().stream().map(row -> row.getValue(0)).toList();
        long nameCount = names.stream().filter(name -> name.equals("Rishi")).count();
        assertEquals(1,nameCount);

        assertEquals("Tobi" , selectQueryResult2.getResultRows().get(0).getValue(0));

    }

    //next tests is to check if select uses index across several tables

    @Test
    void selectQueryUsesIndexAcrossSeveralTables() throws Exception {
        //setup
        DatabaseRunner dbRunner = getDbRunner();
        dbRunner.start();
        String tableName = "students";
        String columnName = "id";
        dbRunner.execute("CREATE TABLE students (id INT, name TEXT, active BOOL);");
        dbRunner.execute("INSERT INTO students (id, name, active) VALUES (1, \"Rishi\", true);");
        dbRunner.execute("INSERT INTO students (id, name, active) VALUES (2, \"Tobi\", true);");
        String tableName2 = "courses";
        String columnName2 = "cid";
        dbRunner.execute("CREATE TABLE courses (cid INT, cname TEXT);");
        dbRunner.execute("INSERT INTO courses (cid, cname) VALUES (1, \"Math\");");
        dbRunner.execute("INSERT INTO courses (cid, cname) VALUES (2, \"Physics\");");

        IndexManager indexManager = dbRunner.getIndexManager();
        assertDoesNotThrow(() ->indexManager.createIndex(tableName, columnName));
        assertDoesNotThrow(() ->indexManager.createIndex(tableName2, columnName2));
        dbRunner.shutdown();

        //now I start it up and load indexes check cache and then i use select query

        DatabaseRunner dbRunner2 = getSpyDbRunner();
        dbRunner2.start();
        SpyIndexManager spyIndexManager = (SpyIndexManager) dbRunner2.getIndexManager();
        assertTrue(spyIndexManager.indexExists(tableName, columnName));
        assertTrue(spyIndexManager.indexExists(tableName2, columnName2));

        QueryResult result1 = dbRunner2.execute("SELECT name,active FROM students WHERE id =1;");
        QueryResult result2 = dbRunner2.execute("Select cname from courses WHERE cid=1;");

        assertTrue(spyIndexManager.searchCalled);
        assertEquals(2,spyIndexManager.countSearchCalled);

        assertInstanceOf(SelectQueryResult.class, result1);
        assertInstanceOf(SelectQueryResult.class, result2);
        SelectQueryResult selectQueryResult = (SelectQueryResult) result1;
        SelectQueryResult selectQueryResult2 = (SelectQueryResult) result2;

        assertEquals(1, selectQueryResult.getRowCount());
        assertEquals(1 ,selectQueryResult2.getRowCount());

        assertEquals("Rishi" , selectQueryResult.getResultRows().getFirst().getValue(0));
        assertEquals("Math" , selectQueryResult2.getResultRows().getFirst().getValue(0));
    }




}
