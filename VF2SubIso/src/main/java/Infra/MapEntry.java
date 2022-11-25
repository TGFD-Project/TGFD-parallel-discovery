package Infra;

import java.io.Serializable;

public class MapEntry implements Serializable {
    public String key;
    public Integer value;

    public MapEntry(String key, Integer value)
    {
        this.key = key;
        this.value = value;
    }

}
