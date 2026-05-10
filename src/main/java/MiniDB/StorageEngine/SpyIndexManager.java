package MiniDB.StorageEngine;

import MiniDB.core.Row;
import MiniDB.core.Value;

import java.util.List;
//this class is a mock class purely used for testing purposes
public class SpyIndexManager extends IndexManager {
    public boolean searchCalled = false;
    public boolean addValueCalled = false;
    public boolean rebuildCalled = false;

    public SpyIndexManager(PageFileStorage storage) {
        super(storage);
    }

    @Override
    public List<RecordId> search(String tableName, String colName, Value value) {
        searchCalled = true;
        return super.search(tableName, colName, value);
    }

    @Override
    public void addValueToIndexWhenInserting(String tableName, String colName , Row row,RecordId recordId) {
        addValueCalled = true;
        super.addValueToIndexWhenInserting(tableName, colName, row, recordId);
    }

    @Override
    public void reBuildIndex(String tableName){
        rebuildCalled = true;
        super.reBuildIndex(tableName);
    }
}
