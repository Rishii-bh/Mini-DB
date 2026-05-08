package MiniDB.query.analyzer;

import MiniDB.StorageEngine.BinaryFileStorage;
import MiniDB.StorageEngine.PageFileStorage;
import MiniDB.core.Column;
import MiniDB.query.rawqueries.CreateTableQuery;
import MiniDB.query.resolved.ResolvedCreateTableQuery;

import java.util.HashSet;
import java.util.List;
import java.util.Set;

public class CreateAnalyzer {

    private final PageFileStorage pageFileStorage;
    public CreateAnalyzer( PageFileStorage pageFileStorage) {
        this.pageFileStorage = pageFileStorage;
    }

    public ResolvedCreateTableQuery resolve(CreateTableQuery createTableQuery) {
       if(createTableQuery.getTableName() == null || createTableQuery.getTableName().isEmpty()){
           throw new RuntimeException("Table name cannot be empty");
       }
       String tableName = createTableQuery.getTableName();
       if(pageFileStorage.tableExists(tableName)){
           throw new RuntimeException("Table " + tableName + " already exists");
       }

        if(createTableQuery.getColumns() == null) {
            throw new RuntimeException("Enter atleast one column");
        }
        Set<String> seen = new HashSet<>();
        for(Column column : createTableQuery.getColumns()) {
           if(!seen.add(column.getCol_name())){
               throw new RuntimeException("Column " + column.getCol_name() + " already exists");
           }
        }
        List<Column> columns = createTableQuery.getColumns();
        return new ResolvedCreateTableQuery(tableName, columns);
    }
}
