import MiniDB.Console.Repl;
import MiniDB.Console.ResultPrinter;
import MiniDB.StorageEngine.*;
import MiniDB.query.QueryEngine;
import MiniDB.sql.SqlRunner;

import java.nio.file.Path;

public class Main {
    public static void main(String[] args) {
        PageFileStorage storageEngine = new PageFileStorage(Path.of("data"));

        QueryEngine queryEngine = new QueryEngine(storageEngine);
        SqlRunner sqlRunner = new SqlRunner(queryEngine);
        ResultPrinter resultPrinter = new ResultPrinter();

        Repl repl = new Repl(sqlRunner, resultPrinter);
        repl.start();
    }
}