package com.vincenzogulisano.usecases.linearroad;

import java.io.Serializable;
import java.util.LinkedList;
import java.util.Queue;

import com.vincenzogulisano.woost.WoostTimeWindow;

import component.operator.in1.aggregate.TimeWindowAddSlide;

/**
 * TODO Please notice: the right way would be to define serialyzers, not put
 * transient here, otherwise the tuple might not be usable in other
 * applications.
 */
public class WindowCountStops implements WoostTimeWindow<TupleInput, TupleCarStops>, Serializable {

    Queue<TupleInput> tuples;
    long currentWinLeftBoundary;
    String key;
    transient int instanceNumber;
    transient int parallelismDegree;
    long tuplesSize;
    long getLastAddedTs;

    // Latency related
    transient long latestStimulus;

    public WindowCountStops() {
        tuples = new LinkedList<>();
        tuplesSize = 0;
    }

    @Override
    public TimeWindowAddSlide<TupleInput, TupleCarStops> factory() {
        return new WindowCountStops();
    }

    @Override
    public WoostTimeWindow<TupleInput, TupleCarStops> woostFactory() {
        return new WindowCountStops();
    }

    @Override
    public boolean isEmpty() {
        return tuples.isEmpty();
    }

    @Override
    public void slideTo(long ts) {
        while (!tuples.isEmpty() && tuples.peek().getTimestamp() < ts) {
            TupleInput t = tuples.poll();
            // 20 has been estimated using LinedListOverheadTest
            tuplesSize -= t.getSize();
        }
        currentWinLeftBoundary = ts;
    }

    @Override
    public void add(TupleInput t) {
        tuples.add(t);
        // 20 has been estimated using LinedListOverheadTest
        tuplesSize += t.getSize();
        getLastAddedTs = t.getTimestamp();
    }

    @Override
    public TupleCarStops getAggregatedResult() {
        boolean stopped = false;
        int stops = 0;
        for (TupleInput t : tuples) {
            if (!stopped && t.getSpeed() == 0) {
                stops++;
                stopped = true;
            } else if (stopped && t.getSpeed() > 0) {
                stopped = false;
            }
        }
        return new TupleCarStops(currentWinLeftBoundary, key, stops, latestStimulus);
    }

    @Override
    public void setKey(String key) {
        this.key = key;
    }

    @Override
    public void setInstanceNumber(int n) {
        instanceNumber = n;
    }

    @Override
    public void setParallelismDegree(int d) {
        parallelismDegree = d;
    }

    @Override
    public int getNumberOfTuples() {
        return tuples.size();
    }

    @Override
    public long getSizeInBytes() {
        return tuplesSize + 144;
    }

	@Override
	public void setLatestStimulus(long latestStimulus) {
		this.latestStimulus = latestStimulus;
	}

	@Override
	public long getLastAddedTs() {
		return getLastAddedTs;
	}

    @Override
    public String toString() {
        return "WindowCountStops [tuples=" + tuples + ", currentWinLeftBoundary=" + currentWinLeftBoundary + ", key="
                + key + ", instanceNumber=" + instanceNumber + ", parallelismDegree=" + parallelismDegree
                + ", tuplesSize=" + tuplesSize + ", getLastAddedTs=" + getLastAddedTs
                + ", latestStimulus=" + latestStimulus + "]";
    }

}
