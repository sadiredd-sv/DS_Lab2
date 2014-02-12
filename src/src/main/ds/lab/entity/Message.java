package ds.lab.entity;

import java.io.Serializable;

public class Message implements Serializable {

    /**
     * Serial version id
     */
    private static final long serialVersionUID = 7628360140269464605L;

    private String mDest = null;

    private String mKind = null;

    private Object mData = null;

    private String mSource = null;

    private int mSeqNum = -1;

    private boolean mIsDup = false;

    public Message(String dest, String kind, Object data) {
        mDest = dest;
        mKind = kind;
        mData = data;
    }

    // These settors are used by MessagePasser.send, not your app
    public void setSource(String source) {
        mSource = source;
    }

    public void setSequenceNumber(int sequenceNumber) {
        mSeqNum = sequenceNumber;
    }

    public void setDuplicate(Boolean dup) {
        mIsDup = dup;
    }

    public String getSource() {
        return mSource;
    }

    public String getDest() {
        return mDest;
    }

    public String getKind() {
        return mKind;
    }

    public Object getData() {
        return mData;
    }

    public int getSequenceNumber() {
        return mSeqNum;
    }

    public boolean isDuplicate() {
        return mIsDup;
    }
}
