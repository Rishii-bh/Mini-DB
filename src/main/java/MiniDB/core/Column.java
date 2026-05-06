package MiniDB.core;

public class Column {
        private final String col_name;
        private final Type type;

        public Column(String col_name, Type type) {
            this.col_name = col_name;
            this.type = type;
        }

        public String getCol_name() {
            return this.col_name;
        }
        public Type getType() {
            return this.type;
        }

}
