package Discovery;

import Infra.ConstantLiteral;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Set;

public class NegativeTGFD {
    private Map.Entry<Set<ConstantLiteral>, ArrayList<Map.Entry<ConstantLiteral, List<Integer>>>> entity;

    public NegativeTGFD(Map.Entry<Set<ConstantLiteral>, ArrayList<Map.Entry<ConstantLiteral, List<Integer>>>> entity)
    {
        this.entity = entity;
    }

    public Map.Entry<Set<ConstantLiteral>, ArrayList<Map.Entry<ConstantLiteral, List<Integer>>>> getEntity() {
        return entity;
    }
}
