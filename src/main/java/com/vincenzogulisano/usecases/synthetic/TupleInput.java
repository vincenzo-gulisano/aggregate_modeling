package com.vincenzogulisano.usecases.synthetic;

import java.io.Serializable;
import java.util.regex.Pattern;

import common.tuple.RichTuple;

public class TupleInput implements RichTuple, Serializable {

  private long timestamp;
  private long stimulus;
  private long key;
  public static final Pattern DELIMITER_PATTERN = Pattern.compile(",");
  private long value;

  public long getSize() {
    return 72;
  }

  public TupleInput() {
  }

  public static TupleInput fromReading(String reading) {
    try {
      String[] tokens = DELIMITER_PATTERN.split(reading.trim());
      return new TupleInput(tokens);
    } catch (Exception exception) {
      throw new IllegalArgumentException(String.format(
          "Failed to parse reading: %s", reading), exception);
    }
  }

  protected TupleInput(String[] readings) {
    this(Long.valueOf(readings[0]), Long.valueOf(readings[1]), Long.valueOf(readings[2]), System.currentTimeMillis());
  }

  protected TupleInput(long time, long key, long value, long stimulus) {
    this.stimulus = stimulus;
    this.timestamp = time;
    this.key = key;
    this.value = value;
  }

  public void setKey(long key) {
    this.key = key;
  }

  public long getValue() {
    return value;
  }

  public void setValue(int value) {
    this.value = value;
  }

  @Override
  public int hashCode() {
    final int prime = 31;
    int result = 1;
    result = prime * result + (int) (timestamp ^ (timestamp >>> 32));
    result = prime * result + (int) (key ^ (key >>> 32));
    result = prime * result + (int) (value ^ (value >>> 32));
    return result;
  }

  @Override
  public boolean equals(Object obj) {
    if (this == obj)
      return true;
    if (obj == null)
      return false;
    if (getClass() != obj.getClass())
      return false;
    TupleInput other = (TupleInput) obj;
    if (timestamp != other.timestamp)
      return false;
    if (key != other.key)
      return false;
    if (value != other.value)
      return false;
    return true;
  }

  @Override
  public String toString() {
    return getTimestamp() + "," + key + "," + value;
  }

  public long getTimestamp() {
    return this.timestamp;
  }

  public String getKey() {
    return String.valueOf(key);
  }

  public long getStimulus() {
    return this.stimulus;
  }

  public void setStimulus(long stimulus) {
    this.stimulus = stimulus;
  }

}