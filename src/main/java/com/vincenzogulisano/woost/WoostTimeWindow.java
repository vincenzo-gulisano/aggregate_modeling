package com.vincenzogulisano.woost;

import common.tuple.RichTuple;
import component.operator.in1.aggregate.TimeWindowAddSlide;

public interface WoostTimeWindow<IN extends RichTuple, OUT extends RichTuple> extends
        TimeWindowAddSlide<IN, OUT> {

    public int getNumberOfTuples();

    public long getSizeInBytes();

    public WoostTimeWindow<IN,OUT> woostFactory();

    public void setLatestStimulus(long latestStimulus);

    public long getLastAddedTs();

}