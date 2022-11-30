package Infra;

import java.io.Serializable;

public abstract class Literal implements Serializable {

    public enum LiteralType
    {
        Constant,
        Variable
    }

    private LiteralType type;

    public Literal(LiteralType t)
    {
        this.type=t;
    }

    public LiteralType getLiteralType() {
        return type;
    }
}
