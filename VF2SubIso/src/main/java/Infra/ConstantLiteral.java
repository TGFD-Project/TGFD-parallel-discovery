package Infra;

import java.io.Serializable;
import java.util.LinkedHashSet;
import java.util.Objects;
import java.util.Set;
import java.util.stream.Collectors;

public class ConstantLiteral extends Literal implements Serializable {

    private String vertexType, attrName, attrValue;
    public ConstantLiteral(String vertexType, String attrName, String attrValue ) {
        super(LiteralType.Constant);
        this.attrName=attrName;
        this.vertexType=vertexType;
        this.attrValue=attrValue;
    }

    public String getAttrName() {
        return attrName;
    }

    public String getAttrValue() {
        return attrValue;
    }

    public String getVertexType() {
        return vertexType;
    }

    public static Set<String> getSignature(Set<ConstantLiteral> literals)
    {
        Set<String> collect = literals.stream()
                .map(X -> X.getAttrName() + X.getAttrValue() + X.getVertexType())
                .sorted()
                .collect(Collectors.toCollection(LinkedHashSet::new));

        return collect;
    }

    public static String getSignature(ConstantLiteral literals)
    {
        return literals.getAttrName() + literals.getAttrValue() + literals.getVertexType();
    }

    @Override
    public String toString() {
        return "ConstantLiteral{" +
                "vertexType='" + vertexType + '\'' +
                ", attrName='" + attrName + '\'' +
                ", attrValue='" + attrValue + '\'' +
                '}';
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        ConstantLiteral that = (ConstantLiteral) o;
        return Objects.equals(vertexType, that.vertexType) && Objects.equals(attrName, that.attrName) && Objects.equals(attrValue, that.attrValue);
    }

    @Override
    public int hashCode() {
        return Objects.hash(vertexType, attrName, attrValue);
    }
}
