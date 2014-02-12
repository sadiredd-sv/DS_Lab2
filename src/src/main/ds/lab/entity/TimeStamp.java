package ds.lab.entity;

import java.io.Serializable;

public abstract class TimeStamp implements Comparable<TimeStamp>, Serializable {

    /**
     * 
     */
    private static final long serialVersionUID = -3640458852347605725L;

    public abstract int compareTo(TimeStamp timestamp);
    
    public abstract Object getValue();
    
    public abstract void setValue(Object value);
}
