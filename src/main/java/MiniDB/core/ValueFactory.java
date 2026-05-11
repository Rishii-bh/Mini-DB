package MiniDB.core;

public final class ValueFactory {
    public static  Value fromLiteral(Object literal, Type type) {
        if(literal instanceof Integer && type == Type.INT){
            return new Value(type, literal);
        }
        if(literal instanceof String && type == Type.TEXT){
            return new Value(type, literal);
        }
        if(literal instanceof Boolean && type == Type.BOOL){
            return new Value(type, literal);
        }
        throw new CoreLayerException("Cannot convert '" + literal + "' to a Value");
    }
}
