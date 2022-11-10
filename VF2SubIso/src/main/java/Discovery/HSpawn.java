package Discovery;

import ICs.TGFD;
import Infra.ConstantLiteral;
import Infra.PatternTreeNode;

import java.util.ArrayList;
import java.util.List;
import java.util.Set;

public class HSpawn {

    private PatternTreeNode patternTreeNode;
    private List<Set<Set<ConstantLiteral>>> matchesPerTimestamps;

    public HSpawn(PatternTreeNode patternTreeNode, List<Set<Set<ConstantLiteral>>> matchesPerTimestamps)
    {
        this.matchesPerTimestamps = matchesPerTimestamps;
        this.patternTreeNode = patternTreeNode;
    }

    public ArrayList<TGFD> performHSPawn()
    {
        ArrayList<TGFD> allTGFDs = new ArrayList<>();

        return allTGFDs;
    }

}
