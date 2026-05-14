package MiniDB.DatabaseRunner;

import MiniDB.Index.IndexManager;
import MiniDB.StorageEngine.PageFileStorage;
import MiniDB.StorageEngine.SchemaManager;
import MiniDB.query.QueryEngine;
import MiniDB.query.QueryResult;
import MiniDB.sql.SqlRunner;

import java.nio.file.Path;

public class DatabaseRunner {
    private final Path dbRoot;
    private final PageFileStorage storage;
    private final SchemaManager schemaManager;
    private final IndexManager indexManager;
    private final QueryEngine queryEngine;
    private final SqlRunner sqlRunner;

    public DatabaseRunner(Path dbRoot) {
        this.dbRoot = dbRoot;
        schemaManager = new SchemaManager(dbRoot);
        storage = new PageFileStorage(schemaManager,dbRoot);
        indexManager = new IndexManager(dbRoot,storage,schemaManager);
        queryEngine = new QueryEngine(storage,indexManager,schemaManager);
        sqlRunner = new SqlRunner(queryEngine);
    }

    public DatabaseRunner(Path dbRoot,SchemaManager schemaManager, PageFileStorage storage,IndexManager indexManager) {
        this.dbRoot = dbRoot;
        this.storage = storage;
        this.schemaManager = schemaManager;
        this.indexManager = indexManager;
        queryEngine = new QueryEngine(storage,indexManager,schemaManager);
        sqlRunner = new SqlRunner(queryEngine);
    }

    public IndexManager getIndexManager() {
        return indexManager;
    }
    public SchemaManager getSchemaManager() {
        return schemaManager;
    }
    public PageFileStorage getStorage() {
        return storage;
    }

    public void start(){
        //eager loading metadata info
        schemaManager.loadAllSchemas();
        indexManager.loadAllIndexes();
    }

    public void shutdown(){
        indexManager.flushDirtyIndexes();
    }

    public QueryEngine getQueryEngine() {
        return queryEngine;
    }

    public QueryResult execute(String query){
        return sqlRunner.execute(query);
    }
}
