package Infra;

import org.jetbrains.annotations.NotNull;

import java.io.Serializable;
import java.util.Objects;

public class DataVertex extends Vertex implements Serializable {

    private String vertexURI="";

    public DataVertex(String uri, String type) {
        super(type.toLowerCase());
        this.vertexURI=uri.toLowerCase();
        this.addAttribute("uri",vertexURI);
        // ???: Is Integer large enough for our use case of possible 10+ million vertices? [2021-02-07]
//        this.hashValue=vertexURI.hashCode();
    }

    @Override
    public String toString() {
        return "DataVertex{" +
                "vertexURI='" + vertexURI + '\'' +
                ", type='" + getTypes() + '\'' +
                ", attributes=" + super.getAllAttributesList() +
                '}';
    }

    public String getVertexURI() {
        return vertexURI;
    }

    @Override
    public boolean isMapped(Vertex v) {
        if(v instanceof DataVertex)
            return false;
        if (super.getTypes().containsAll(v.getTypes()) || v.getTypes().iterator().next().equals("_")) {
            if (super.getAllAttributesNames().containsAll(v.getAllAttributesNames())) {
                for (Attribute attr : v.getAllAttributesList())
                    if(attr instanceof MultiValueAttribute && !((MultiValueAttribute) attr).exists(super.getAttributeValueByName(attr.getAttrName())))
                    {
                       return false;
                    }
                    else if (!attr.isNULL() && !super.getAttributeValueByName(attr.getAttrName()).equals(attr.getAttrValue())) {
                        return false;
                    }
                return true;
            } else {
                return false;
            }
        } else {
            return false;
        }
    }

    @Override
    public int compareTo(@NotNull Vertex o) {
        if(o instanceof DataVertex)
        {
            DataVertex v=(DataVertex) o;
            return this.vertexURI.compareTo(v.vertexURI);
        }
        else
            return 0;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (!(o instanceof DataVertex)) return false;
        DataVertex that = (DataVertex) o;
        return Objects.equals(vertexURI, that.vertexURI);
    }

    @Override
    public int hashCode() {
        return Objects.hash(vertexURI);
    }

}
