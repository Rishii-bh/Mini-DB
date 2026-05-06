package MiniDB.query.condition;

import MiniDB.core.Column;
import MiniDB.core.Type;

public class Condition {
    private final Column expression1;
    private final Operator operator;
    private final Object value;

    public Condition(Column expression1, Operator operator, Object value) {
        this.expression1 = expression1;
        this.operator = operator;
        this.value = value;
    }

    public Column getExpression1() {
        return expression1;
    }
    public Operator getOperator() {
        return operator;
    }
    public Object getValue() {
        return value;
    }

    public boolean isValidOperator() {
        if(this.expression1.getType() == Type.BOOL && operator != Operator.EQUALTO && operator != Operator.NOT_EQUALTO){
            return false;
        }
        if(this.expression1.getType() == Type.TEXT && operator != Operator.EQUALTO && operator != Operator.NOT_EQUALTO){
            return false;
        }
        return true;
    }

    public boolean isValidValue() {
        Type type = this.expression1.getType();

        return switch (type) {
            case BOOL -> this.value instanceof Boolean;
            case TEXT -> this.value instanceof String;
            case INT -> this.value instanceof Integer;
            case REAL -> this.value instanceof Double;
        };
    }

    public boolean evaluate(Object row_value) {
        return switch (operator) {
            case GREATERTHAN -> compareValues(row_value, this.value) > 0;
            case LESSERTHAN -> compareValues(row_value, this.value) < 0;
            case EQUALTO -> compareValues(row_value, this.value) == 0;
            case GREATEREQUALTO -> compareValues(row_value, this.value) >= 0;
            case LESSEREQUALTO -> compareValues(row_value, this.value) <= 0;
            case NOT_EQUALTO -> compareValues(row_value, this.value) != 0;
        };
    }


    private int compareValues(Object left , Object right) {
        if(left instanceof String && right instanceof String){
            return ((String)left).compareTo((String)right);
        }
        if(left instanceof Integer && right instanceof Integer){
            return ((Integer)left).compareTo((Integer)right);
        }
        if(left instanceof Double && right instanceof Double){
            return ((Double)left).compareTo((Double)right);
        }
        if(left instanceof Boolean && right instanceof Boolean){
            return ((Boolean)left).compareTo((Boolean)right);
        }
        throw new IllegalArgumentException(
                "Cannot compare values of different types: "
                        + left.getClass().getSimpleName()
                        + " and "
                        + right.getClass().getSimpleName()
        );
    }
}
