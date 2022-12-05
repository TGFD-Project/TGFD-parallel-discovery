package Discovery;

import Infra.ConstantLiteral;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Set;

public class NoDeltaTGFD {
    private Map.Entry<Set<ConstantLiteral>, ArrayList<Map.Entry<ConstantLiteral, List<Integer>>>> entity;

    public NoDeltaTGFD(Map.Entry<Set<ConstantLiteral>, ArrayList<Map.Entry<ConstantLiteral, List<Integer>>>> entity)
    {
        this.entity = entity;
    }

    public Map.Entry<Set<ConstantLiteral>, ArrayList<Map.Entry<ConstantLiteral, List<Integer>>>> getEntity() {
        return entity;
    }
}
