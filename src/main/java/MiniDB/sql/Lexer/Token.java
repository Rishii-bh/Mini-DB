package MiniDB.sql.Lexer;

public final class Token {
    private final TokenType type;
    private final String lexeme;
    private final Object literal;
    private final int position;

    public Token(TokenType type, String lexeme, Object literal, int position) {
        this.type = type;
        this.lexeme = lexeme;
        this.literal = literal;
        this.position = position;
    }
    public Token(){
        this.type = null;
        this.lexeme = null;
        this.literal = null;
        this.position = 0;
    }

    public TokenType getType() {
        return type;
    }

    public String getLexeme() {
        return lexeme;
    }

    public Object getLiteral() {
        return literal;
    }

    public int getPosition() {
        return position;
    }

    @Override
    public String toString() {
        return "Token{" +
                "type=" + type +
                ", lexeme='" + lexeme + '\'' +
                ", literal=" + literal +
                ", position=" + position +
                '}';
    }
}