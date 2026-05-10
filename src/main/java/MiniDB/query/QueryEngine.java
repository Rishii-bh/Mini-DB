package MiniDB.query;

import MiniDB.StorageEngine.BinaryFileStorage;
import MiniDB.StorageEngine.IndexManager;
import MiniDB.StorageEngine.PageFileStorage;
import MiniDB.query.analyzer.CreateAnalyzer;
import MiniDB.query.analyzer.DeleteAnalyzer;
import MiniDB.query.analyzer.InsertAnalyzer;
import MiniDB.query.analyzer.SelectAnalyzer;
import MiniDB.query.rawqueries.CreateTableQuery;
import MiniDB.query.rawqueries.DeleteQuery;
import MiniDB.query.rawqueries.InsertQuery;
import MiniDB.query.rawqueries.SelectQuery;
import MiniDB.query.resolved.*;
import MiniDB.query.results.CreateTableQueryResult;
import MiniDB.query.results.DeleteQueryResult;
import MiniDB.query.results.InsertQueryResult;
import MiniDB.query.results.SelectQueryResult;

public class QueryEngine {
    private final SelectAnalyzer selectAnalyzer;
    private final QueryExecutor executor;
    private final InsertAnalyzer insertAnalyzer;
    private final CreateAnalyzer createAnalyzer;
    private final DeleteAnalyzer deleteAnalyzer;
    private final IndexManager indexManager;

    public QueryEngine(PageFileStorage pageFileStorage,IndexManager indexManager) {
        this.indexManager = indexManager;
        this.executor = new QueryExecutor(pageFileStorage,indexManager);
        this.selectAnalyzer = new SelectAnalyzer(pageFileStorage);
        this.insertAnalyzer = new InsertAnalyzer(pageFileStorage);
        this.createAnalyzer = new CreateAnalyzer(pageFileStorage);
        this.deleteAnalyzer = new DeleteAnalyzer(pageFileStorage);
    }

    public QueryResult execute(Query query){
        if (query instanceof SelectQuery selectQuery) {
           return execute(selectQuery);
        }

        if (query instanceof InsertQuery insertQuery) {
           return execute(insertQuery);
        }

        if (query instanceof CreateTableQuery createTableQuery) {
            return execute(createTableQuery);
        }

        if (query instanceof DeleteQuery deleteQuery) {
           return execute(deleteQuery);
        }

        throw new IllegalArgumentException(
                "Unsupported query type: " + query.getClass().getSimpleName()
        );
    }

    public SelectQueryResult execute(SelectQuery query) {
        ResolvedSelectQuery resolved = selectAnalyzer.resolve(query);
        return executor.executeSelectQuery(resolved);
    }

    public InsertQueryResult execute(InsertQuery query) {
        ResolvedInsertQuery resolved = insertAnalyzer.resolve(query);
        return executor.executeInsertQuery(resolved);
    }

    public CreateTableQueryResult execute(CreateTableQuery query) {
        ResolvedCreateTableQuery resolved = createAnalyzer.resolve(query);
        return executor.executeCreateQuery(resolved);
    }

    public DeleteQueryResult execute(DeleteQuery query) {
        ResolvedDeleteQuery resolved = deleteAnalyzer.resolve(query);
        return executor.executeDeleteQuery(resolved);
    }
}
