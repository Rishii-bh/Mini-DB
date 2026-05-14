package UnitTestsForInternalWorking;

import MiniDB.Index.BinaryIndexSerializer;
import MiniDB.Index.IndexException;
import MiniDB.Index.IndexValueRecordId;
import MiniDB.StorageEngine.RecordId;
import MiniDB.core.Type;
import MiniDB.core.Value;
import MiniDB.core.ValueFactory;
import org.junit.jupiter.api.Test;

import java.io.IOException;

import static org.junit.jupiter.api.Assertions.*;

public class IndexSerializerTest {
    @Test
    void serializeAndDeserializeIndexesProduceSameResult() throws IOException {
        Value value = ValueFactory.fromLiteral(1, Type.INT);
        RecordId rid = new RecordId(1,1);
        BinaryIndexSerializer serializer = new BinaryIndexSerializer();
        byte[] serializedIndex = assertDoesNotThrow(() -> serializer.serialize(value, rid));
        IndexValueRecordId vrid = serializer.deserialize(serializedIndex, Type.INT);
        assertEquals(value , vrid.value());
        assertEquals(rid, vrid.recordId());
    }

    @Test
    void corruptValueDoesNotSerialize() throws IOException {
        Value value = new Value(Type.INT, "Rishi");
        RecordId rid = new RecordId(1,1);
        BinaryIndexSerializer serializer = new BinaryIndexSerializer();
        assertThrows(IndexException.class, () -> serializer.serialize(value, rid));
    }
}
