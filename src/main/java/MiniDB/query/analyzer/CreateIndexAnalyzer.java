package MiniDB.query.analyzer;

import MiniDB.Index.IndexManager;
import MiniDB.StorageEngine.PageFileStorage;
import MiniDB.query.resolved.ResolvedCreateIndexQuery;

public class CreateIndexAnalyzer {
    private final PageFileStorage pageFileStorage;
    private final IndexManager indexManager;

    public CreateIndexAnalyzer(PageFileStorage pageFileStorage,IndexManager indexManager) {
        this.pageFileStorage = pageFileStorage;
        this.indexManager = indexManager;
    }

    public ResolvedCreateIndexQuery resolve(String tableName, String columnName) {
        if(indexManager.indexExists(tableName, columnName)) {
           throw new RuntimeException("Index already exists");
        }
        indexManager.createIndex(tableName, columnName);

        return new ResolvedCreateIndexQuery(tableName, columnName);
    }
}
