package MiniDB.core;

import java.util.ArrayList;
import java.util.List;

public class Row {
    private final List<Object> values;

    public Row(List<Object> values) {
        this.values = new ArrayList<>(values);
    }

    public Object getValue(int index) {
        return values.get(index);
    }

    public int size() {
        return values.size();
    }
    public List<Object> getRow() {return values;}

    @Override
    public String toString() {
        return values.toString();
    }
}
