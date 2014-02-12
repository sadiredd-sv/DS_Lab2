package ds.lab.core.clock;

import ds.lab.entity.TimeStamp;
import ds.lab.entity.VectorTimeStamp;

public class VectorClock extends Clock {
	
	private static final int TIMESTAMP_START_FROM = 0;

	private static final int TIMESTAMP_DIFF = 1;

	private VectorTimeStamp mCurrentTimeStamp = null;
	
	private int mProcessIndex;
	
	private int mNumProcesses;
	
	public VectorClock(int processIndex, int numProcesses) {
		mProcessIndex = processIndex;
		mNumProcesses = numProcesses;
		mCurrentTimeStamp = new VectorTimeStamp(TIMESTAMP_START_FROM, processIndex, numProcesses);
	}
	
	@Override
	public TimeStamp getTimeStamp() {
		return mCurrentTimeStamp;
	}
	
    @Override
    public TimeStamp generateTimeStamp() {
        mCurrentTimeStamp = mCurrentTimeStamp.next(TIMESTAMP_DIFF);
        return mCurrentTimeStamp;
    }

    @Override
    public void syncClock(TimeStamp timeStamp) {
    	mCurrentTimeStamp = mCurrentTimeStamp
    			.sync((VectorTimeStamp)timeStamp)
    			.next(TIMESTAMP_DIFF);
    }
}
