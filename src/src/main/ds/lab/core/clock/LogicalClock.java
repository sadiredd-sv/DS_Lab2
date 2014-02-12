package ds.lab.core.clock;

import ds.lab.entity.LogicalTimeStamp;
import ds.lab.entity.TimeStamp;

public class LogicalClock extends Clock {

    private LogicalTimeStamp mCurrentTimeStamp = null;

    private static final int TIMESTAMP_START_FROM = -1;

    private static final int TIMESTAMP_DIFF = 1;

    public LogicalClock() {
        mCurrentTimeStamp = new LogicalTimeStamp(TIMESTAMP_START_FROM);
    }
    
    @Override
    public TimeStamp getTimeStamp() {
    	return mCurrentTimeStamp;
    }

    @Override
    public TimeStamp generateTimeStamp() {
        if (mCurrentTimeStamp != null) {
            mCurrentTimeStamp = new LogicalTimeStamp(
                    (Integer)mCurrentTimeStamp.getValue() + TIMESTAMP_DIFF);
        }
        return mCurrentTimeStamp;
    }

    @Override
    public void syncClock(TimeStamp timeStamp) {
        if (mCurrentTimeStamp != null && timeStamp != null) {
            int maxValue = Math.max((Integer)mCurrentTimeStamp.getValue(),
                    (Integer)((LogicalTimeStamp) timeStamp).getValue());
            mCurrentTimeStamp.setValue(maxValue + TIMESTAMP_DIFF);
        }
    }

}
