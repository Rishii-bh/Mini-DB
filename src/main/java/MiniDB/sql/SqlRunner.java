package MiniDB.sql;

import MiniDB.query.Query;
import MiniDB.query.QueryEngine;
import MiniDB.query.QueryResult;
import MiniDB.sql.Lexer.Lexer;
import MiniDB.sql.Lexer.Token;
import MiniDB.sql.Parser.Parser;

import java.util.List;

public class SqlRunner {
    private final QueryEngine queryEngine;

    public SqlRunner(QueryEngine queryEngine) {
        this.queryEngine = queryEngine;
    }

    public QueryResult execute(String sql) {
        Lexer lexer = new Lexer(sql);
        List<Token> tokens = lexer.scanTokens();
        Parser parser = new Parser(tokens);
        Query query = parser.parseStatement();
        return queryEngine.execute(query);
    }
}
