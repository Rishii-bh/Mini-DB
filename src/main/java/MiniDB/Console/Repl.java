package MiniDB.Console;

import MiniDB.query.QueryResult;
import MiniDB.sql.SqlRunner;

import java.util.Scanner;

public class Repl {
    private final SqlRunner sqlRunner;
    private final ResultPrinter resultPrinter;

    public Repl(SqlRunner sqlRunner, ResultPrinter resultPrinter) {
        this.sqlRunner = sqlRunner;
        this.resultPrinter = resultPrinter;
    }

    public void start() {
        Scanner scanner = new Scanner(System.in);

        while (true) {
            System.out.print("minidb> ");
            String input = scanner.nextLine();

            if (input.equalsIgnoreCase("exit")) {
                break;
            }

            if (input.isBlank()) {
                continue;
            }

            try {
                QueryResult result = sqlRunner.execute(input);
                resultPrinter.print(result);
            } catch (RuntimeException e) {
                System.out.println("Error: " + e.getMessage());
            }
        }
    }
}
