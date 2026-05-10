package MiniDB.query.analyzer;

import MiniDB.StorageEngine.IndexManager;
import MiniDB.StorageEngine.PageFileStorage;
import MiniDB.query.resolved.ResolvedCreateIndexQuery;

public class CreateIndexAnalyzer {
    private final PageFileStorage pageFileStorage;
    private final IndexManager indexManager;

    public CreateIndexAnalyzer(PageFileStorage pageFileStorage) {
        this.pageFileStorage = pageFileStorage;
        this.indexManager = new IndexManager(pageFileStorage);
    }

    public ResolvedCreateIndexQuery resolve(String tableName, String columnName) {
        if(indexManager.hasIndexKey(tableName, columnName)) {
           throw new RuntimeException("Index already exists");
        }
        indexManager.createIndex(tableName, columnName);

        return new ResolvedCreateIndexQuery(tableName, columnName);
    }
}
