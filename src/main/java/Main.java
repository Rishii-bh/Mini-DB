import MiniDB.Console.Repl;
import MiniDB.Console.ResultPrinter;
import MiniDB.DatabaseRunner.DatabaseRunner;
import MiniDB.Index.IndexManager;
import MiniDB.StorageEngine.*;
import MiniDB.query.QueryEngine;
import MiniDB.query.QueryResult;
import MiniDB.sql.SqlRunner;

import java.nio.file.Path;

public class Main {
    public static void main(String[] args) {
        Path root = Path.of("data");
        DatabaseRunner dbRunner = new DatabaseRunner(root);
        dbRunner.start();
        Runtime.getRuntime().addShutdownHook(new Thread(dbRunner::shutdown));
        ResultPrinter resultPrinter = new ResultPrinter();
        Repl repl = new Repl(dbRunner, resultPrinter);
        repl.start();
    }
}