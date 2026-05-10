package MiniDB.StorageEngine;

import MiniDB.core.Row;

public record RowWithRecordId(Row row, RecordId recordId) {
}
