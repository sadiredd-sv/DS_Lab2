package ds.lab.core.clock;

import ds.lab.entity.TimeStamp;

public class ClockService {

    protected Clock mClock = null;

    public ClockService(Clock clock) {
        mClock = clock;
    }

    public synchronized Clock getClock() {
        return mClock;
    }
    
    public synchronized TimeStamp getTimeStamp() {
    	if (mClock != null) {
    		return mClock.getTimeStamp();
    	}
    	return null;
    }

    public synchronized TimeStamp generateTimeStamp() {
        if (mClock != null) {
            return mClock.generateTimeStamp();
        }
        return null;
    }

    public synchronized void synchClock(TimeStamp timeStamp) {
        if (mClock != null) {
            mClock.syncClock(timeStamp);
        }
    }
}
