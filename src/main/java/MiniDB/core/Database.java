package MiniDB.core;

import java.util.HashMap;
import java.util.Map;

public class Database {
    Map<String , Table> tables;
    public Database() {
        tables = new HashMap<>();
    }


    public void createTable(String table_name, Table table) {
        if(tables.containsKey(table_name)) {
            throw new RuntimeException("Table " + table_name + " already exists");
        }
        if(table_name.isEmpty()) {
            throw new RuntimeException("Table name is empty");
        }
        tables.put(table_name, table);
    }

    public Table getTable(String table_name) {

       return tables.get(table_name);
    }
    public void printDatabase(String table_name) {
        Table table = tables.get(table_name);
        if(table == null) {
            throw new RuntimeException("Table " + table_name + " not found");
        }
        table.printTable();
    }
}
