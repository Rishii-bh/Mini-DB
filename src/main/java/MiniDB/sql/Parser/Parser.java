package MiniDB.sql.Parser;

import MiniDB.sql.Lexer.Token;
import MiniDB.sql.Lexer.TokenType;
import MiniDB.core.Column;
import MiniDB.core.Type;
import MiniDB.query.condition.Operator;
import MiniDB.query.Query;
import MiniDB.query.rawqueries.*;

import java.util.ArrayList;
import java.util.List;

public class Parser {
    private final List<Token> tokens;
    public Parser(List<Token> tokens) {
        if (tokens == null || tokens.isEmpty()) {
            throw new ParserException("Token list cannot be null or empty");
        }

        if (tokens.get(tokens.size() - 1).getType() != TokenType.EOF) {
            throw new ParserException("Token list must end with EOF");
        }

        this.tokens = tokens;
    }
    //GRAMMAR ACCEPTED
    //Select <col_names> from <table_name> Optional(where <condition>);
    //Create Table <table_name> (<ident> type,...);
    //Insert into <table_name> (<col_namws> values <values list>
    //delete from <table_name> where condition

    private int current = 0;

    public Query parseStatement(){
        if(match(TokenType.SELECT)){
            return parseSelectQuery();
        }
        if(match(TokenType.CREATE)){
            return parseCreateTableQuery();
        }
        if(match(TokenType.INSERT)){
            return parseInsertQuery();
        }
        if(match(TokenType.DELETE)){
            return parseDeleteQuery();
        }
        throw  new ParserException("Unexpected token at position "+current);

    }

    private CreateTableQuery parseCreateTableQuery(){
        consume(TokenType.TABLE , "Expected keyword Table");
        Token tableNameToken = consume(TokenType.IDENTIFIER , "Expected Identifier");
        consume(TokenType.LEFT_PAREN , "Expected left parenthesis");
        List<Column> columns = parseCreateColumns();
        consume(TokenType.RIGHT_PAREN , "Expected right parenthesis");
        consume(TokenType.SEMICOLON , "Expected ; at end of statement");
        consume(TokenType.EOF , "Expected EOF");
        return new CreateTableQuery(tableNameToken.getLexeme(),columns);
    }

    private List<Column> parseCreateColumns(){
        List<Column> columns = new ArrayList<>();
        columns.add(parseColumnDef());
        while (match(TokenType.COMMA)){
            columns.add(parseColumnDef());
        }
        return columns;
    }

    private Column parseColumnDef() {
        Token colNameToken = consume(TokenType.IDENTIFIER , "Expected Identifier");
        Type type = parseType();
        return new Column(colNameToken.getLexeme(),type);
    }

    private Type parseType(){
        if (match(TokenType.TYPE_INT)) {
            return Type.INT;
        }
        if (match(TokenType.TYPE_BOOL)) {
            return Type.BOOL;
        }
        if(match(TokenType.TYPE_TEXT)){
            return Type.TEXT;
        }
        throw new ParserException("Requires Identifier Type at "+current);
    }

    private SelectQuery parseSelectQuery(){
        List<String> colNames = parseColNames();
        consume(TokenType.FROM,"Expected keyword From");
        Token tableNameToken = consume(TokenType.IDENTIFIER , "Expected Identifier");
        if(match(TokenType.WHERE)){
            Token conditionToken = consume(TokenType.IDENTIFIER , "Expected Identifier");
            Operator operator = parseOperator();
            Object value = getLiteral();
            consume(TokenType.SEMICOLON , "Expected semicolon");
            consume(TokenType.EOF , "Expected EOF");
            RawConditionQuery condition = new RawConditionQuery(conditionToken.getLexeme(),operator,value);
            return new SelectQuery(tableNameToken.getLexeme(),colNames,condition);
        }
        else if(match(TokenType.SEMICOLON)){
            consume(TokenType.EOF , "Expected EOF");
            return new SelectQuery(tableNameToken.getLexeme(),colNames);
        }
        else{
            throw new ParserException("Unexpected End Of input "+current);
        }

    }

    private Object getLiteral(){
       if(check(TokenType.INT_LITERAL)){
           Token literalToken = consume(TokenType.INT_LITERAL , "Expected integer");
           return literalToken.getLiteral();
       }
       if(check(TokenType.STRING_LITERAL)){
           Token literalToken = consume(TokenType.STRING_LITERAL , "Expected string");
           return literalToken.getLiteral();
       }
       if(check(TokenType.BOOL_LITERAL)){
           Token literalToken = consume(TokenType.BOOL_LITERAL , "Expected bool");
           return literalToken.getLiteral();
       }
       throw error("Expected Literal Value at "+current);
    }

    private Operator parseOperator() {
        if(match(TokenType.EQUAL)){
            return Operator.EQUALTO;
        }
        if(match(TokenType.NOT_EQUAL)){
            return Operator.NOT_EQUALTO;
        }
        if(match(TokenType.GREATER)){
            return Operator.GREATERTHAN;
        }
        if(match(TokenType.LESS)){
            return Operator.LESSERTHAN;
        }
        if(match(TokenType.GREATER_EQUAL)){
            return Operator.GREATEREQUALTO;
        }
        if(match(TokenType.LESS_EQUAL)){
            return Operator.LESSEREQUALTO;
        }
        throw  error("Unexpected token at position "+current);
    }

    private List<String> parseColNames() {
        List<String> colNames = new ArrayList<>();
        Token colNameToken = consume(TokenType.IDENTIFIER , "Expected Identifier");
        colNames.add(colNameToken.getLexeme());
        while (match(TokenType.COMMA)){
            colNameToken = consume(TokenType.IDENTIFIER , "Expected Identifier");
            colNames.add(colNameToken.getLexeme());
        }
        return colNames;
    }

    private InsertQuery parseInsertQuery(){
        consume(TokenType.INTO,"Expected keyword Into");
        Token tableNameToken = consume(TokenType.IDENTIFIER , "Expected Identifier");
        consume(TokenType.LEFT_PAREN , "Expected left parenthesis");
        List<String> colNames = parseColNames();
        consume(TokenType.RIGHT_PAREN , "Expected right parenthesis");
        consume(TokenType.VALUES , "Expected keyword Values");
        consume(TokenType.LEFT_PAREN , "Expected left parenthesis");
        List<Object> literalValues  = getInsertValues();
        consume(TokenType.RIGHT_PAREN , "Expected right parenthesis");
        consume(TokenType.SEMICOLON , "Expected semicolon");
        consume(TokenType.EOF , "Expected EOF");
        return new InsertQuery(tableNameToken.getLexeme(),colNames,literalValues);
    }

    private List<Object> getInsertValues(){
        List<Object> values = new ArrayList<>();
        if(match(TokenType.INT_LITERAL,TokenType.BOOL_LITERAL,TokenType.STRING_LITERAL)){
            values.add(prev().getLiteral());
            while (match(TokenType.COMMA)){
                if(match(TokenType.INT_LITERAL,TokenType.BOOL_LITERAL,TokenType.STRING_LITERAL)){
                    values.add(prev().getLiteral());
                }
                else{
                    throw  error("Expects a literal value at "+current);
                }
            }
        }
        else{
            throw error("Unexpected token requires a literal value"+current);
        }
        return values;
    }
    private DeleteQuery parseDeleteQuery(){
        consume(TokenType.FROM,"Expected keyword From");
        Token tableNameToken = consume(TokenType.IDENTIFIER , "Expected Identifier");
        consume(TokenType.WHERE,"Expected keyword Where");
        Token conditionToken = consume(TokenType.IDENTIFIER , "Expected Identifier");
        Operator operator = parseOperator();
        Object value = getLiteral();
        consume(TokenType.SEMICOLON , "Expected semicolon");
        consume(TokenType.EOF , "Expected EOF");
        RawConditionQuery rawConditionQuery = new RawConditionQuery(conditionToken.getLexeme(),operator,value);
        return new DeleteQuery(tableNameToken.getLexeme(),rawConditionQuery);

    }

    private Token consume(TokenType expected, String message) {
        if (check(expected)) {
            return advance();
        }

        throw error(message);
    }

    private boolean match(TokenType... types) {
        for (TokenType type : types) {
            if (check(type)) {
                advance();
                return true;
            }
        }

        return false;
    }

    private boolean check(TokenType type){
        return peek().getType() == type;
    }

    private Token advance() {
        if(!isAtEnd()){
            current++;
        }
        return prev();
    }

    private Token peek() {
        return tokens.get(current);
    }
    private Token prev(){
        return tokens.get(current-1);
    }

    private boolean isAtEnd() {
        return peek().getType() == TokenType.EOF;
    }

    private ParserException error(String message) {
        Token token = peek();

        return new ParserException(
                message + " at token '" + token.getLexeme() + "' (" + token.getType() + ")"
        );
    }
}
