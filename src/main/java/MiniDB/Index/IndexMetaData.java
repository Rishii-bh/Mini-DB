package MiniDB.Index;

import MiniDB.core.Type;

public record IndexMetaData(String tableName , String colName, Type colType, IndexType indexType , String fileName) {
}
