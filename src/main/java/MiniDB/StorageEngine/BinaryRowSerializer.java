package MiniDB.StorageEngine;

import MiniDB.core.Column;
import MiniDB.core.Row;
import MiniDB.core.Schema;
import MiniDB.core.Type;

import java.io.*;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.List;

public class BinaryRowSerializer {

    public byte[] serialize(Row row, Schema schema) {
       if(row == null){
           throw new StorageException("Row is null");
       }
       if(schema == null){
           throw new StorageException("Schema is null");
       }

       if(row.size() != schema.size()){
           throw new StorageException("Row size does not match schema size");
       }

       try{
           ByteArrayOutputStream byteBuffer = new ByteArrayOutputStream();
           DataOutputStream out = new DataOutputStream(byteBuffer);

           for(int i = 0; i < schema.size(); i++){
               Object value = row.getValue(i);
               Type type = schema.getColumn(i).getType();
               writeValue(out,type,value);
           }
           out.flush();
           return byteBuffer.toByteArray();
       }catch (IOException e){
           throw new StorageException("Coudldnt write row to byte" + e);
       }

    }

    private void writeValue(DataOutputStream out, Type type, Object value) throws IOException {
        if(value == null){
            throw new StorageException("Value to be serialized is null");
        }
        switch (type){
            case INT: writeInt(out,value);
            break;
            case TEXT:writeText(out,value);
            break;
            case BOOL:writeBoolean(out,value);
            break;
        }
    }

    private void writeInt(DataOutputStream out, Object value) throws IOException {
        if(!(value instanceof Integer intValue)){
            throw new StorageException("Value to be serialized is not an integer");
        }
        out.writeInt(intValue);
    }

    private void writeText(DataOutputStream out, Object value) throws IOException {
        if(!(value instanceof String textValue)){
            throw new StorageException("Value to be serialized is not a string");
        }
        byte[] textBytes = textValue.getBytes(StandardCharsets.UTF_8);
        out.writeInt(textBytes.length);
        out.write(textBytes);

    }
    private void writeBoolean(DataOutputStream out, Object value) throws IOException {
        if(!(value instanceof Boolean booleanValue)){
            throw new StorageException("Value to be serialized is not a boolean");
        }
        out.writeBoolean(booleanValue);

    }

    public Row deserialize(byte[] bytes, Schema schema) {
        if(bytes == null){
            throw new StorageException("Row is null");
        }
        if(schema == null){
            throw new StorageException("Schema is null");
        }
        try{
            ByteArrayInputStream inputStream = new ByteArrayInputStream(bytes);
            DataInputStream in = new DataInputStream(inputStream);
            List<Object> values = new ArrayList<>();

            for(Column column : schema.getColumns()){
                Type type = column.getType();
                Object value = readValue(in , type);
                values.add(value);
            }
            if(in.available()!=0){
                throw new StorageException("Row bytes contain extra byte input" + in.available());
            }
            return new Row(values);

        }catch (IOException e){
            throw new StorageException("Coudlnt read row from byte" , e);
        }
    }

    private Object readValue(DataInputStream in, Type type) throws IOException {
       return switch (type){
           case INT -> in.readInt();
           case TEXT -> readText(in);
           case BOOL -> in.readBoolean();
           default -> throw new StorageException("Type not supported yet!!");
       };
    }

    private String readText(DataInputStream in) throws IOException {
        int length = in.readInt();
        if(length <=0){
            throw new StorageException("Invalid length of string");
        }
        byte[] bytes = new byte[length];
        in.readFully(bytes);
        return new String(bytes, StandardCharsets.UTF_8);
    }

}
