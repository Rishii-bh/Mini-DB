package MiniDB.Console;

import MiniDB.DatabaseRunner.DatabaseRunner;
import MiniDB.query.QueryResult;
import MiniDB.sql.SqlRunner;

import java.util.Scanner;

public class Repl {
    private final DatabaseRunner dbRunner;
    private final ResultPrinter resultPrinter;

    public Repl(DatabaseRunner dbRunner, ResultPrinter resultPrinter) {
        this.dbRunner = dbRunner;
        this.resultPrinter = resultPrinter;
    }

    public void start() {
        Scanner scanner = new Scanner(System.in);

        while (true) {
            System.out.print("minidb> ");
            String input = scanner.nextLine();

            if (input.equalsIgnoreCase("exit")) {
                dbRunner.shutdown();
                break;
            }

            if (input.isBlank()) {
                continue;
            }

            try {
                QueryResult result = dbRunner.execute(input);
                resultPrinter.print(result);
            } catch (RuntimeException e) {
                System.out.println("Error: " + e.getMessage());
            }
        }
    }
}
