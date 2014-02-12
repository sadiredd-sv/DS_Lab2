package ds.lab.entity;

public class TimeStampedMessage extends Message {

    private TimeStamp mTimeStamp = null;

    /**
     * Serial version id
     */
    private static final long serialVersionUID = 1796423964496706687L;

    public TimeStampedMessage(String dest, String kind, Object data) {
        super(dest, kind, data);
    }

    public void setTimeStamp(TimeStamp timeStamp) {
        mTimeStamp = timeStamp;
    }

    public TimeStamp getTimeStamp() {
        return mTimeStamp;
    }
}
