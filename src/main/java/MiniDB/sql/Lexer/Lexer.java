package MiniDB.sql.Lexer;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

public final class Lexer {
    private static final Map<String, TokenType> KEYWORDS = Map.ofEntries(
            Map.entry("SELECT", TokenType.SELECT),
            Map.entry("FROM", TokenType.FROM),
            Map.entry("WHERE", TokenType.WHERE),
            Map.entry("INSERT", TokenType.INSERT),
            Map.entry("INTO", TokenType.INTO),
            Map.entry("VALUES", TokenType.VALUES),
            Map.entry("CREATE", TokenType.CREATE),
            Map.entry("TABLE", TokenType.TABLE),
            Map.entry("DELETE", TokenType.DELETE),

            Map.entry("INT", TokenType.TYPE_INT),
            Map.entry("TEXT", TokenType.TYPE_TEXT),
            Map.entry("BOOL", TokenType.TYPE_BOOL)
    );

    private final String input;
    private final List<Token> tokens = new ArrayList<>();

    private int start = 0;
    private int current = 0;

    public Lexer(String input) {
        if (input == null) {
            throw new IllegalArgumentException("Source input cannot be null");
        }

        this.input = input;
    }

    public List<Token> scanTokens() {
        while (!isAtEnd()) {
            start = current;
            scanToken();
        }

        tokens.add(new Token(TokenType.EOF, "", null, current));
        return List.copyOf(tokens);
    }

    private void scanToken() {
        char c = advance();

        switch (c) {
            case '(' -> addToken(TokenType.LEFT_PAREN);
            case ')' -> addToken(TokenType.RIGHT_PAREN);
            case ',' -> addToken(TokenType.COMMA);
            case ';' -> addToken(TokenType.SEMICOLON);
            case '=' -> addToken(TokenType.EQUAL);

            case '>' -> addToken(match('=') ? TokenType.GREATER_EQUAL : TokenType.GREATER);
            case '<' -> addToken(match('=') ? TokenType.LESS_EQUAL : TokenType.LESS);

            case '!' -> {
                if (match('=')) {
                    addToken(TokenType.NOT_EQUAL);
                } else {
                    throw error("Unexpected character '!'. Did you mean '!='?");
                }
            }

            case '"' -> scanString();

            case ' ', '\r', '\t', '\n' -> {
                // Ignore whitespace.
            }

            default -> {
                if (isDigit(c)) {
                    scanNumber();
                } else if (isIdentifierStart(c)) {
                    scanIdentifier();
                } else {
                    throw error("Unexpected character: '" + c + "'");
                }
            }
        }
    }

    private void scanIdentifier() {
        while (isIdentifierPart(peek())) {
            advance();
        }

        String text = input.substring(start, current);
        String upper = text.toUpperCase();

        if (upper.equals("TRUE") || upper.equals("FALSE")) {
            addToken(TokenType.BOOL_LITERAL, Boolean.parseBoolean(text));
            return;
        }

        TokenType type = KEYWORDS.getOrDefault(upper, TokenType.IDENTIFIER);
        addToken(type);

    }

    private void scanNumber() {
        while (isDigit(peek())) {
            advance();
        }

        String text = input.substring(start, current);

        try {
            int value = Integer.parseInt(text);
            addToken(TokenType.INT_LITERAL, value);
        } catch (NumberFormatException e) {
            throw error("Integer literal is too large: " + text);
        }
    }

    private void scanString() {
        while (peek() != '"' && !isAtEnd()) {
            advance();
        }

        if (isAtEnd()) {
            throw error("Unterminated string literal");
        }

        // Consume closing quote.
        advance();

        String value = input.substring(start + 1, current - 1);
        addToken(TokenType.STRING_LITERAL, value);
    }

    private char advance() {
        return input.charAt(current++);
    }

    private boolean match(char expected) {
        if (isAtEnd()) {
            return false;
        }

        if (input.charAt(current) != expected) {
            return false;
        }

        current++;
        return true;
    }

    private char peek() {
        if (isAtEnd()) {
            return '\0';
        }

        return input.charAt(current);
    }

    private boolean isAtEnd() {
        return current >= input.length();
    }

    private void addToken(TokenType type) {
        addToken(type, null);
    }

    private void addToken(TokenType type, Object literal) {
        String lexeme = input.substring(start, current);
        tokens.add(new Token(type, lexeme, literal, start));
    }

    private LexerException error(String message) {
        return new LexerException(message + " at position " + current);
    }

    private boolean isIdentifierStart(char c) {
        return Character.isLetter(c) || c == '_';
    }

    private boolean isIdentifierPart(char c) {
        return Character.isLetterOrDigit(c) || c == '_';
    }

    private boolean isDigit(char c) {
        return c >= '0' && c <= '9';
    }
}