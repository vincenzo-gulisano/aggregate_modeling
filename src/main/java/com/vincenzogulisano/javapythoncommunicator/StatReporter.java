package com.vincenzogulisano.javapythoncommunicator;

import com.vincenzogulisano.util.EpisodesLogger;

public interface StatReporter {

    void report(long ts, String id, double value);

    public void setResetRequest();

    public boolean getResetAcknowledged();

    public void setResetCompleted();

    /**
     * This method is used to give a token to the StatReporter it can use to send a
     * state/reward to the agent
     * 
     * @param clockTimeBarrier Optional barrier that specifies when (in clock time)
     *                         a state reward can be sent. Set to -1 is there is no
     *                         barrier
     * @param eventTimeBarrier Optional barrier that specifies when (in event time)
     *                         a state reward can be sent. Set to -1 is there is no
     *                         barrier
     */
    public void addSendStateToken(long clockTimeBarrier, long eventTimeBarrier, long dValue);

    public void registerLogger(EpisodesLogger logger);

    public void close();

}
