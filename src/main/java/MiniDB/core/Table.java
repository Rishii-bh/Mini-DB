package MiniDB.core;

import java.util.ArrayList;
import java.util.List;

public class Table {
    private final String name;
    private final Schema schema;
    private final List<Row> rows;

    public Table(String name, Schema schema){
        this.name = name;
        this.schema = schema;
        this.rows = new ArrayList<>();
    }

    public void insert(Row row){
        validate(row);
        rows.add(row);
    }

    private void validate(Row row){
        if(row.size() != schema.size()){
            throw new RuntimeException("Row size mismatch");
        }
        for(int i = 0; i < schema.size(); i++){
            Type expectedType = schema.getColumn(i).getType();
            Object rowValue = row.getValue(i);
            if(!matches(expectedType, rowValue)){
                throw new RuntimeException("Row value mismatch");
            }
        }
    }

    private boolean matches(Type type, Object value){
        return switch (type){
            case INT -> value instanceof Integer;
            case TEXT -> value instanceof String;
            case BOOL -> value instanceof Boolean;
            case REAL -> value instanceof Double;
        };
    }

    public Object getValue(Row row, String column_name){
        int index = schema.getColumnIndex(column_name);
        return row.getValue(index);
    }

    public Schema getSchema() {
        return this.schema;
    }

    public Row getRow(int index) {
        return this.rows.get(index);
    }

    public int getRowCount() {
        return this.rows.size();
    }

    public String getName() {
        return this.name;
    }

    public void delete(int index){
        rows.remove(index);
    }


    public void printTable(){
        for(Column col : schema.getColumns()){
            System.out.print(col.getCol_name());
            System.out.print("\t ");
        }
        System.out.println();
        for(Row row : rows){
            for(int i=0;i<row.size();i++){
                System.out.print(row.getValue(i).toString());
                System.out.print("\t|");
            }
            System.out.println();

        }
    }


}
