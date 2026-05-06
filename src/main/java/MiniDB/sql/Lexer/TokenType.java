package MiniDB.sql.Lexer;

public enum TokenType {
    // Keywords
    SELECT,
    FROM,
    WHERE,
    INSERT,
    INTO,
    VALUES,
    CREATE,
    TABLE,
    DELETE,

    // SQL types
    TYPE_INT,
    TYPE_TEXT,
    TYPE_BOOL,

    // Identifiers and literals
    IDENTIFIER,
    INT_LITERAL,
    STRING_LITERAL,
    BOOL_LITERAL,

    // Punctuation
    LEFT_PAREN,
    RIGHT_PAREN,
    COMMA,
    SEMICOLON,

    // Operators
    EQUAL,
    GREATER,
    GREATER_EQUAL,
    LESS,
    LESS_EQUAL,
    NOT_EQUAL,

    // End of input
    EOF
}