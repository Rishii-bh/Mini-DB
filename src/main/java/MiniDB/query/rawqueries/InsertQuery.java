package MiniDB.query.rawqueries;

import MiniDB.query.Query;

import java.util.List;

public class InsertQuery implements Query {
    private final String table_name;
    private final List<String> col_names;
    private final List<Object> values;

    public InsertQuery(String table_name, List<String> col_names, List<Object> values) {
        this.table_name = table_name;
        this.col_names = col_names;
        this.values = values;
    }

    public String getTable_name() {
        return table_name;
    }
    public List<String> getCol_names() {
        return col_names;
    }
    public List<Object> getValues() {
        return values;
    }

}
