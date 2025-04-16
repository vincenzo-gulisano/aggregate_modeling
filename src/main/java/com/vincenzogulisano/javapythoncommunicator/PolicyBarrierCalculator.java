package com.vincenzogulisano.javapythoncommunicator;

public class PolicyBarrierCalculator {

    private final long wallclockTimeBarrier;
    private final long eventTimeBarrier;

    private PolicyBarrierCalculator(long wallclockTimeBarrier, long eventTimeBarrier) {
        this.wallclockTimeBarrier = wallclockTimeBarrier;
        this.eventTimeBarrier = eventTimeBarrier;
    }

    private static long getWSWACeil(long wa, long ws) {
        return (long) Math.ceil((double) ws / (double) wa);
    }

    private static long getContributingWindows(long ts, long wa, long ws) {
        return ts % wa >= ws % wa && ws % wa != 0L ? (getWSWACeil(wa, ws) - 1) : getWSWACeil(wa, ws);
    }

    private static long getEarliestWinStartTS(long ts, long wa, long ws) {
        return (long) Math.max((double) ((ts / wa - getContributingWindows(ts, wa, ws) + 1L) * wa), 0.0);
    }

    public static PolicyBarrierCalculator getBarriers(PolicyBarrier policyBarrier, long latestClockTime,
            long latestEventTime, long wa, long ws) {
        switch (policyBarrier) {
            case WELOB:
                return new PolicyBarrierCalculator(-1, -1);
            case ELOB:
                return new PolicyBarrierCalculator(latestClockTime, -1);
            case LOB:
                return new PolicyBarrierCalculator(latestClockTime, latestEventTime + 1);
            case WELAW:

                return new PolicyBarrierCalculator(latestClockTime,
                        getEarliestWinStartTS(latestEventTime, wa, ws) + ws);

            default:
                throw new RuntimeException("Unknown PolicyBarrier " + policyBarrier);
        }
    }

    public long getWallclockTimeBarrier() {
        return wallclockTimeBarrier;
    }

    public long getEventTimeBarrier() {
        return eventTimeBarrier;
    }

}
