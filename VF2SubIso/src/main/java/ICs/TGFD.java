package ICs;

import Infra.DataDependency;
import Infra.Delta;
import Infra.VF2PatternGraph;

public class TGFD {

    private Delta delta;
    private DataDependency dependency;
    private VF2PatternGraph pattern;
    private String name="";
    private Double support=-1.0;
    private Double patternSupport=-1.0;

    public TGFD (VF2PatternGraph pattern, Delta delta, DataDependency dependency, String name)
    {
        this.delta=delta;
        this.pattern=pattern;
        this.dependency=dependency;
        this.name=name;
    }

    public TGFD (VF2PatternGraph pattern, Delta delta, DataDependency dependency, double support, double patternSupport, String name)
    {
        this.delta = delta;
        this.pattern = pattern;
        this.dependency = dependency;
        this.support = support;
        this.patternSupport = patternSupport;
        this.name=name;
    }

    public TGFD ()
    {
        this.dependency=new DataDependency();
    }

    public void setDelta(Delta delta) {
        this.delta = delta;
    }

    public void setDependency(DataDependency dependency) {
        this.dependency = dependency;
    }

    public void setPattern(VF2PatternGraph pattern) {
        this.pattern = pattern;
    }

    public VF2PatternGraph getPattern() {
        return pattern;
    }

    public DataDependency getDependency() {
        return dependency;
    }

    public Delta getDelta() {
        return delta;
    }

    public String getName() {
        return name;
    }

    public void setName(String name) {
        this.name = name;
    }

    public Double getSupport() {
        return support;
    }

    @Override
    public String toString() {
        return "TGFD{" +
                "\n pattern=" + pattern +
                "\n patternSupport=" + patternSupport +
                "\n dependency=" + dependency +
                "\n delta=" + delta +
                "\n support=" + support +
                '}';
    }

}
