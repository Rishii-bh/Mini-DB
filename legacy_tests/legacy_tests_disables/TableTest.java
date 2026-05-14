package legacy_tests_disables;

import MiniDB.core.*;
import org.junit.jupiter.api.Test;

import java.util.List;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;

public class TableTest {

    @Test
    void insertValidRowAddsRowToTable() {
        Schema schema = new Schema(List.of(
                new Column("id", Type.INT),
                new Column("Name", Type.TEXT)
        ));

        Table table = new Table("students", schema);

        table.insert(new Row(List.of(1, "Rishi")));

        Assertions.assertEquals(1, table.getRowCount());
    }

    @Test
    void insertWrongColumnCountThrowsException() {
        Schema schema = new Schema(List.of(
                new Column("id", Type.INT),
                new Column("Name", Type.TEXT)
        ));

        Table table = new Table("students", schema);

        Assertions.assertThrows(RuntimeException.class, () -> {
            table.insert(new Row(List.of(1)));
        });
    }
}
