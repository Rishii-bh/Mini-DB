import MiniDB.StorageEngine.*;
import MiniDB.core.*;
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
        IndexManager indexManager = new IndexManager(pageFileStorage);
        QueryEngine queryEngine = new QueryEngine(pageFileStorage , indexManager);
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
    @Test
    void insertRowWithRecordIdAndFetch(){
        PageFileStorage pageFileStorage = new PageFileStorage(tempDir);
        Schema schema = new Schema(List.of(
                new Column("id", Type.INT),
                new Column("name", Type.TEXT),
                new Column("active", Type.BOOL)
        ));
        String tableName = "students";
        pageFileStorage.createTable(tableName, schema);
        Row row = new Row(List.of(1 ,"Rishi",true));
//        Row row2 = new Row(List.of(2 ,"Alex",false));
        RecordId recordId = pageFileStorage.insertRowWithRecordId(tableName,row);
//        RecordId recordId2 = pageFileStorage.insertRowWithRecordId(tableName,row2);
        Row persistedRow = pageFileStorage.getRowByRecordId(tableName,recordId);
        assertEquals(row.getRow(),persistedRow.getRow());

    }

    //INDEX BASED TESTS

    @Test
    void indexTestShouldCreateIndexAndFetchRecordIdsThrowWhenColNameMissing() throws Exception {
        SqlRunner runner = newRunner();
        runner.execute("CREATE TABLE students (id INT, name TEXT, active BOOL);");
        runner.execute("Insert into students (id, name, active) values (1, \"Rishi\", true);");
        runner.execute("Insert into students (id, name, active) values (2, \"Sara\", false);");
        runner.execute("Insert into students (id, name, active) values (1, \"Alex\", true);");
        PageFileStorage pageFileStorage = new PageFileStorage(tempDir);
        IndexBuilder indexBuilder = new IndexBuilder(pageFileStorage);
        String tableName = "students";
        String columnName = "active";
        String missingColumnName = "xyz";
        InMemoryIndex index = indexBuilder.build(tableName, columnName);
        Value value = new Value(Type.BOOL , true);

        int numberOfActiveRows = index.get(value).size();
        List<RecordId> recordIds = index.get(value);
        List<Row> rows = new ArrayList<>();
        for(RecordId recordId : recordIds) {
            rows.add(pageFileStorage.getRowByRecordId(tableName, recordId));
        }
        assertEquals("Rishi", rows.get(0).getValue(1));
        assertEquals("Alex", rows.get(1).getValue(1));


        assertEquals(2,numberOfActiveRows);

        assertThrows(CoreLayerException.class ,() -> indexBuilder.build(tableName, missingColumnName));

        Value notIndexedColumnValue = new Value(Type.INT , 1);
        int numberOfNotIndexedRows = index.get(notIndexedColumnValue).size();
        assertEquals(0,numberOfNotIndexedRows);

    }

    @Test
    void selectWhereQueryUsesIndex() throws Exception {
        PageFileStorage pageFileStorage = new PageFileStorage(tempDir);
        SpyIndexManager indexManager = new SpyIndexManager(pageFileStorage);
        QueryEngine queryEngine = new QueryEngine(pageFileStorage, indexManager);
        SqlRunner runner = new SqlRunner(queryEngine);
        runner.execute("CREATE TABLE students (id INT, name TEXT, active BOOL);");
        runner.execute("Insert into students (id, name, active) values (1, \"Rishi\", true);");
        runner.execute("Insert into students (id, name, active) values (2, \"Sara\", false);");
        runner.execute("Insert into students (id, name, active) values (1, \"Alex\", true);");
        indexManager.createIndex("students" , "id");
        QueryResult result = runner.execute("Select name from students where id = 1;");
        assertInstanceOf(SelectQueryResult.class, result);
        SelectQueryResult selectQueryResult = (SelectQueryResult) result;
        assertEquals(2,selectQueryResult.getRowCount());
        assertTrue(indexManager.searchCalled);

    }
    @Test
    void selectWhereQueryUsesFullRowScanWhenNotIndexed() throws Exception {
        PageFileStorage pageFileStorage = new PageFileStorage(tempDir);
        SpyIndexManager indexManager = new SpyIndexManager(pageFileStorage);
        QueryEngine queryEngine = new QueryEngine(pageFileStorage, indexManager);
        SqlRunner runner = new SqlRunner(queryEngine);
        runner.execute("CREATE TABLE students (id INT, name TEXT, active BOOL);");
        runner.execute("Insert into students (id, name, active) values (1, \"Rishi\", true);");
        runner.execute("Insert into students (id, name, active) values (2, \"Sara\", false);");
        runner.execute("Insert into students (id, name, active) values (1, \"Alex\", true);");
        indexManager.createIndex("students" , "name");
        QueryResult result = runner.execute("Select name from students where id = 1;");
        assertInstanceOf(SelectQueryResult.class, result);
        SelectQueryResult selectQueryResult = (SelectQueryResult) result;
        assertEquals(2,selectQueryResult.getRowCount());
        assertFalse(indexManager.searchCalled);

    }

    @Test
    void insertQueryOnIndexedTableShouldCreateIndexForNewRow() throws Exception {
        PageFileStorage pageFileStorage = new PageFileStorage(tempDir);
        SpyIndexManager indexManager = new SpyIndexManager(pageFileStorage);
        QueryEngine queryEngine = new QueryEngine(pageFileStorage, indexManager);
        SqlRunner runner = new SqlRunner(queryEngine);
        runner.execute("CREATE TABLE students (id INT, name TEXT, active BOOL);");
        runner.execute("Insert into students (id, name, active) values (1, \"Rishi\", true);");
        runner.execute("Insert into students (id, name, active) values (2, \"Sara\", false);");
        runner.execute("Insert into students (id, name, active) values (1, \"Alex\", true);");

        indexManager.createIndex("students" , "id");
        runner.execute("Insert into students (id, name, active) values (3, \"Jones\", false);");
        assertTrue(indexManager.addValueCalled);

    }

    @Test
    void deleteQueryOnIndexedTableShouldRebuildIndex() throws Exception {
        PageFileStorage pageFileStorage = new PageFileStorage(tempDir);
        SpyIndexManager indexManager = new SpyIndexManager(pageFileStorage);
        QueryEngine queryEngine = new QueryEngine(pageFileStorage, indexManager);
        SqlRunner runner = new SqlRunner(queryEngine);
        runner.execute("CREATE TABLE students (id INT, name TEXT, active BOOL);");
        runner.execute("Insert into students (id, name, active) values (1, \"Rishi\", true);");
        runner.execute("Insert into students (id, name, active) values (2, \"Sara\", false);");
        runner.execute("Insert into students (id, name, active) values (2, \"Alex\", true);");

        indexManager.createIndex("students" , "id");
        runner.execute("Delete from students where id = 1;");
        QueryResult result = runner.execute("Select name from students where id = 2;");
        assertInstanceOf(SelectQueryResult.class, result);
        SelectQueryResult selectQueryResult = (SelectQueryResult) result;
        assertEquals(2,selectQueryResult.getRowCount());
        assertTrue(indexManager.rebuildCalled);
        assertTrue(indexManager.searchCalled);
    }





}
