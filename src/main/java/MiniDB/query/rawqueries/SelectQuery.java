package MiniDB.query.rawqueries;

import MiniDB.query.Query;

import java.util.ArrayList;
import java.util.List;
import java.util.Optional;

public class SelectQuery implements Query {
    private final String table_name;
    private final List<String> col_names;
    private final RawConditionQuery condition;

    public SelectQuery(String table_name, List<String> col_names) {
        this.table_name = table_name;
        this.col_names = new ArrayList<>(col_names);
        this.condition =null;

    }

    public SelectQuery(String table_name, List<String> col_names, RawConditionQuery condition) {
        this.table_name = table_name;
        this.col_names = new ArrayList<>(col_names);
        this.condition = condition;
    }

    public String getTableName() {
        return this.table_name;
    }
    public List<String> getColNames() {
        return this.col_names;
    }

    public Optional<RawConditionQuery> getCondition() {
        return Optional.ofNullable(condition);
    }

}
