package ds.lab.core.clock;

import ds.lab.util.Constant;

/**
 * Clock service factory
 */
public final class ClockServiceFactory {

    /**
     * Private constructor
     */
    private ClockServiceFactory() {

    }

    /**
     * 
     * @param type
     * @return
     */
    public static ClockService createClockService(int type, int processIndex, int numProcesses) {
        Clock clock = createClock(type, processIndex, numProcesses);
    	return new ClockService(clock);
    }

    private static Clock createClock(int type, int processIndex, int numProcesses) {
        Clock clock = null;
        switch (type) {
        case Constant.LOGICAL_CLOCK:
            clock = new LogicalClock();
            break;
        case Constant.VECTOR_CLOCK:
            clock = new VectorClock(processIndex, numProcesses);
            break;
        default:
            throw new IllegalArgumentException();
        }
        return clock;
    }
}
