package MiniDB.StorageEngine;

import MiniDB.core.Row;
import MiniDB.core.Schema;
import MiniDB.core.Table;

import java.util.List;

public interface StorageEngine {
     void createTable(String table_name, Schema schema);

     boolean tableExists(String table_name);

     Schema getSchema(String table_name);

     List<Row> getRows(String table_name);

     void insertRow(String table_name, Row row);

     void replaceRows(String table_name, List<Row> rows);
}
