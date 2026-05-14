package MiniDB.Index;

import MiniDB.StorageEngine.RecordId;
import MiniDB.StorageEngine.StorageException;
import MiniDB.core.Type;
import MiniDB.core.Value;

import java.io.*;
import java.nio.charset.StandardCharsets;

public class BinaryIndexSerializer {

    public byte[] serialize(Value key, RecordId recordId) throws IOException {
        if (recordId == null) {
            throw new IndexException("RecordId is null!");
        }
        if (key == null) {
            throw new IndexException("Key is null!");
        }
        try {
            ByteArrayOutputStream bos = new ByteArrayOutputStream();
            DataOutputStream dos = new DataOutputStream(bos);
            writeKeyLengthAndValue(key, dos);
            writeRecordId(recordId, dos);
            dos.flush();
            return bos.toByteArray();
        } catch (IOException e) {
            throw new IndexException("Error serializing recordId!!" ,e);
        }
    }
    private void writeKeyLengthAndValue(Value key , DataOutputStream dos) throws IOException {
        Type type = key.type();
        switch (type) {
                case INT: writeInt(dos,key.value());
                    break;
                case TEXT:writeText(dos,key.value());
                    break;
                case BOOL:writeBoolean(dos,key.value());
                    break;
            }
        }


    private void writeInt(DataOutputStream out, Object value) throws IOException {
        if(!(value instanceof Integer intValue)){
            throw new IndexException("Value is not an integer!");
        }
        out.writeInt(4);
        out.writeInt(intValue);
    }

    private void writeText(DataOutputStream out, Object value) throws IOException {
        if(!(value instanceof String stringValue)){
            throw new IndexException("Value is not a String!");
        }
        byte[] textBytes = stringValue.getBytes(StandardCharsets.UTF_8);
        out.writeInt(textBytes.length);
        out.write(textBytes);
    }

    private void writeBoolean(DataOutputStream out, Object value) throws IOException {
        if(!(value instanceof Boolean booleanValue)){
            throw new IndexException("Value is not a Boolean!");
        }
        out.writeInt(1);
        out.writeBoolean(booleanValue);
    }

    private void writeRecordId(RecordId recordId, DataOutputStream out) throws IOException {
        int pageNO = recordId.PageNo();
        out.writeInt(pageNO);
        int slotId = recordId.SlotId();
        out.writeInt(slotId);
    }

    public IndexValueRecordId deserialize(byte[] bytes, Type type) throws IOException{
        if(bytes == null){
            throw new IndexException("Bytes is null!");
        }
        Value value = null;
        try {
            DataInputStream dis = new DataInputStream(new ByteArrayInputStream(bytes));
            switch (type){
                case INT -> value = readInteger(dis);
                case TEXT -> value = readText(dis);
                case BOOL ->  value = readBoolean(dis);
                default -> throw new StorageException("Type not supported yet!!");
            };
            if(dis.available() != 8){
                throw new StorageException("Index Value Contains extra bytes");
            }
            if(value == null){
                throw new IndexException("Index Value is null!");
            }
            int PageNo = dis.readInt();
            int SlotId = dis.readInt();
            RecordId recordId = new RecordId(PageNo,SlotId);
            return new IndexValueRecordId(value,recordId);
        }catch (IOException e){
            throw new IndexException("Error deserializing Index Value!!",e);
        }

    }

    private Value readInteger(DataInputStream dis) throws IOException {
        int length = dis.readInt();
        if(length != 4){
            throw new IndexException("Integer value is too short!");
        }
        Integer intValue = dis.readInt();
        return new Value(Type.INT, intValue);
    }

    private Value readText(DataInputStream dis) throws IOException {
        int length = dis.readInt();
        if(length <0){
            throw new IndexException("Invalid length of text value !");
        }
        byte[] value = new byte[length];
        dis.readFully(value);
        String stringValue = new String(value , StandardCharsets.UTF_8);
        return new Value(Type.TEXT, stringValue);
    }

    private Value readBoolean(DataInputStream dis) throws IOException {
        int length = dis.readInt();
        if(length != 1){
            throw new IndexException("Invalid length of boolean value !");
        }
        Boolean boolValue = dis.readBoolean();
        return new Value(Type.BOOL, boolValue);
    }
}
