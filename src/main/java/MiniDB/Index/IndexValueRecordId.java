package MiniDB.Index;

import MiniDB.StorageEngine.RecordId;
import MiniDB.core.Value;

public record IndexValueRecordId(Value value , RecordId recordId) {
}
