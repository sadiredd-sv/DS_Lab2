package ds.lab.entity;

public class LogicalTimeStamp extends TimeStamp {

    /**
     * 
     */
    private static final long serialVersionUID = -8552215488560861206L;

    private int mValue = 0;

    public LogicalTimeStamp(int value) {
        mValue = value;
    }

    @Override
    public Object getValue() {
        return mValue;
    }

    @Override
    public void setValue(Object value) {
        mValue = (Integer) value;
    }

    @Override
    public int compareTo(TimeStamp ts) {
        if (ts != null) {
            return mValue - (Integer) ((LogicalTimeStamp) ts).getValue();
        }
        return -1;
    }

    @Override
    public String toString() {
        return String.format("LogicalTimeStamp< " + mValue + " >");
    }
}
