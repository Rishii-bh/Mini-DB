package MiniDB.StorageEngine;

import MiniDB.core.Schema;

import java.io.IOException;
import java.nio.file.DirectoryStream;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.HashMap;
import java.util.List;
import java.util.Locale;
import java.util.Map;

public class SchemaManager {
    private final Path rootDb;
    private final SchemaSerializer serializer;
    private final Map<String , Schema> schemaCache;

    public SchemaManager(Path rootDb) {
        this.rootDb = rootDb;
        this.serializer = new SchemaSerializer();
        schemaCache = new HashMap<>();
    }

    public void loadAllSchemas() {
        try {
            if(!Files.exists(rootDb)){
                Files.createDirectories(rootDb);
                return;
            }
            try(DirectoryStream<Path> tables = Files.newDirectoryStream(rootDb)){
                for(Path tableDir : tables){
                    if(!Files.isDirectory(tableDir)){
                        continue;
                    }
                    Path schemaPath = tableDir.resolve("schema.meta");
                    if(!Files.exists(schemaPath)){
                        continue;
                    }
                    String tableName = tableDir.getFileName().toString();
                    Schema tableSchema = loadSchema(schemaPath);
                    schemaCache.put(tableName, tableSchema);
                }
            }
        }catch(IOException e){
            throw new StorageException("Could not load all schemas", e);
        }

    }
    public void registerSchema(String tableName, Schema schema) {
        schemaCache.put(normalize(tableName), schema);
    }

    public Schema getSchema(String tableName) {
        Schema schema = schemaCache.get(normalize(tableName));
        if(schema == null){
            throw new StorageException("Could not find schema for table " + tableName);
        }
        return schema;
    }

    private Schema loadSchema(Path schemaPath) {
        try{
            List<String> lines = Files.readAllLines(schemaPath);
            return serializer.deserialize(lines);
        }catch (IOException e){
            throw new StorageException("Could not read schema File", e);
        }
    }

    private String normalize(String name){
        return name.toLowerCase(Locale.ROOT);
    }



}
