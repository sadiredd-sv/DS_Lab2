package ds.lab.core.clock;

import ds.lab.entity.TimeStamp;

public abstract class Clock {
	
	public abstract TimeStamp getTimeStamp();

    public abstract TimeStamp generateTimeStamp();

    public abstract void syncClock(TimeStamp timeStamp);
}
