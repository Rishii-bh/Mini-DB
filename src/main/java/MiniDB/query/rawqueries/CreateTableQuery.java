package MiniDB.query.rawqueries;

import MiniDB.core.Column;
import MiniDB.query.Query;

import java.util.List;

public class CreateTableQuery implements Query {
        private final String tableName;
        private final List<Column> columns;

        public CreateTableQuery(String tableName, List<Column> columns) {
            this.tableName = tableName;
            this.columns = List.copyOf(columns);
        }

        public String getTableName() {
            return tableName;
        }

        public List<Column> getColumns() {
            return columns;
        }
}

