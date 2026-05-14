package MiniDB.Index;

import MiniDB.core.Column;
import MiniDB.core.Type;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardOpenOption;
import java.util.ArrayList;
import java.util.List;

public class IndexCatalog {
    private static final String separator = "\\|";
    private static final String catalogSeparator = "|";

    public void write(Path catalog , IndexMetaData metaData) throws IOException {
        try{
            Files.createDirectories(catalog.getParent());
            String line = toLine(metaData);
            Files.writeString(catalog, line+ System.lineSeparator(), StandardOpenOption.CREATE, StandardOpenOption.APPEND);
        }catch (IOException e){
            throw new IndexException("Error writing index catalog", e);
        }
    }

    private String toLine(IndexMetaData indexMetaData){
        return indexMetaData.tableName()
                + catalogSeparator
                +indexMetaData.colName()
                + catalogSeparator
                +indexMetaData.colType().name()
                + catalogSeparator
                +indexMetaData.indexType().name()
                + catalogSeparator
                +indexMetaData.fileName();
    }

    public List<IndexMetaData> read(Path catalog) throws IOException {
        if(!Files.exists(catalog)){
            return new ArrayList<>();
        }
        try{
            List<String> lines = Files.readAllLines(catalog);
            List<IndexMetaData> indexMetaData = new ArrayList<>();
            for(String line : lines){
                indexMetaData.add(parseString(line));
            }
            return indexMetaData;
        }catch (IOException e){
            throw new IndexException("Error reading index catalog", e);
        }

    }

    private IndexMetaData parseString(String line){
        String[] parts = line.split(separator);

        if(parts.length != 5){
            throw new IndexException("Index MetaData is corrupted");
        }
        String tableName = parts[0];
        String colName = parts[1];
        String colType = parts[2];
        String indexType = parts[3];
        String fileName = parts[4];
        Type type = parseColumnType(colType);
        IndexType indexType1 = parseIndexType(indexType);
        return new IndexMetaData(tableName , colName , type , indexType1 , fileName) ;
    }

    private IndexType parseIndexType(String indexType){
        if(indexType.equalsIgnoreCase(IndexType.FLAT.name())){
            return IndexType.FLAT;
        }
        throw new IndexException("Index Type is incorrect");
    }

    private Type parseColumnType(String colType){
       if(colType.equalsIgnoreCase(Type.INT.name())){
           return Type.INT;
       }
       if(colType.equalsIgnoreCase(Type.TEXT.name())){
           return Type.TEXT;
       }
       if(colType.equalsIgnoreCase(Type.BOOL.name())){
           return Type.BOOL;
       }
       throw new IndexException("Column Type is incorrect");
    }
}
