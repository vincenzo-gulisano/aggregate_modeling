package com.vincenzogulisano.usecases.linearroad;


import java.util.Objects;

import common.tuple.BaseRichTuple;

public class TupleCarStops extends BaseRichTuple {

  private int stops;

  public TupleCarStops(long time, String key, int stops, long stimulus) {
    super(stimulus,time,key);
    this.stops = stops;
  }

  public int getStops() {
    return stops;
  }

  public void setStops(int stops) {
    this.stops = stops;
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) {
      return true;
    }
    if (o == null || getClass() != o.getClass()) {
      return false;
    }
    if (!super.equals(o)) {
      return false;
    }
    TupleCarStops that = (TupleCarStops) o;
    return stops == that.stops;
  }

  @Override
  public int hashCode() {
    return Objects.hash(super.hashCode(), stops);
  }

  @Override
  public String toString() {
    return getTimestamp() + "," + key + "," + stops;
  }
}