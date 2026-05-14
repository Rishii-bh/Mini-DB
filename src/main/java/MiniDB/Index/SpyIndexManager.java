package MiniDB.Index;

import MiniDB.StorageEngine.PageFileStorage;
import MiniDB.StorageEngine.RecordId;
import MiniDB.StorageEngine.RowWithRecordId;
import MiniDB.StorageEngine.SchemaManager;
import MiniDB.core.Row;
import MiniDB.core.Value;

import java.nio.file.Path;
import java.util.LinkedHashSet;
import java.util.List;

//this class is a spy class purely used for testing purposes
public class SpyIndexManager extends IndexManager {
    public boolean searchCalled = false;
    public boolean onInsertCalled = false;
    public boolean onDeleteCalled = false;
    public int countSearchCalled = 0;

    public SpyIndexManager(Path dbRoot , PageFileStorage storage , SchemaManager schemaManager) {
        super(dbRoot ,storage,schemaManager);
    }

    @Override
    public LinkedHashSet<RecordId> search(String tableName, String colName, Value value) {
        searchCalled = true;
        countSearchCalled++;
        return super.search(tableName, colName, value);
    }

    @Override
    public void onInsert(String tableName, Row row, RecordId recordId) {
        onInsertCalled = true;
        super.onInsert(tableName,row, recordId);
    }
    @Override
    public void onDelete(String tableName, List<RowWithRecordId> rows) {
        onDeleteCalled = true;
        super.onDelete(tableName, rows);
    }

//    @Override
//    public void reBuildIndex(String tableName){
//        rebuildCalled = true;
//        super.reBuildIndex(tableName);
//    }
}
