package com.vincenzogulisano.usecases.synthetic;

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
public class WindowSynthetic implements WoostTimeWindow<TupleInput, TupleInput>, Serializable {

    Queue<TupleInput> tuples;
    long currentWinLeftBoundary;
    long key;
    transient int instanceNumber;
    transient int parallelismDegree;
    long tuplesSize;
    long getLastAddedTs;
    long squaresSum;

    // Latency related
    transient long latestStimulus;

    public WindowSynthetic() {
        tuples = new LinkedList<>();
        tuplesSize = 0;
        squaresSum = 0;
    }

    @Override
    public TimeWindowAddSlide<TupleInput, TupleInput> factory() {
        return new WindowSynthetic();
    }

    @Override
    public WoostTimeWindow<TupleInput, TupleInput> woostFactory() {
        return new WindowSynthetic();
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
            squaresSum -=  Math.sqrt(Math.pow(t.getValue(), 2));
        }
        currentWinLeftBoundary = ts;
    }

    @Override
    public void add(TupleInput t) {
        squaresSum += Math.sqrt(Math.pow(t.getValue(), 2));
        tuples.add(t);
        // 20 has been estimated using LinedListOverheadTest
        tuplesSize += t.getSize();
        getLastAddedTs = t.getTimestamp();
    }

    @Override
    public TupleInput getAggregatedResult() {
        
        return new TupleInput(currentWinLeftBoundary, Long.valueOf(key), squaresSum, latestStimulus);
    }

    @Override
    public void setKey(String key) {
        this.key = Long.valueOf(key);
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
        return tuplesSize + 104;
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
