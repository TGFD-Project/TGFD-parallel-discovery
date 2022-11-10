package Discovery;

import Infra.AttributeDependency;
import Infra.ConstantLiteral;
import Infra.LiteralTreeNode;
import Infra.PatternTreeNode;

import java.util.List;
import java.util.Set;

public class DeltaDiscovery {


    private PatternTreeNode patternNode;
    private LiteralTreeNode literalTreeNode;
    private AttributeDependency literalPath;
    private List<Set<Set<ConstantLiteral>>> matchesPerTimestamps;

    public DeltaDiscovery()
    {

    }

}
