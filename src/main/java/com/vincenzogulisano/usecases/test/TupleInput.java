package com.vincenzogulisano.usecases.test;

import java.io.Serializable;
import java.util.regex.Pattern;

import common.tuple.RichTuple;

public class TupleInput implements RichTuple, Serializable {

  private long timestamp;
  private long stimulus;
  private long key;
  public static final Pattern DELIMITER_PATTERN = Pattern.compile(",");
  private long value;

  /**
   * Returns the size of the TupleInput object in bytes.
   * This value is hardcoded as 72, representing the estimated memory footprint.
   */
  public long getSize() {
    return 72;
  }
  
  public static TupleInput fromReading(String reading) {
    try {
      String[] tokens = DELIMITER_PATTERN.split(reading.trim());
      if (tokens.length != 3) {
        throw new IllegalArgumentException(String.format(
            "Invalid input: expected 3 tokens but got %d in reading: %s", tokens.length, reading));
      }
      return new TupleInput(tokens);
    } catch (Exception exception) {
      throw new IllegalArgumentException(String.format(
          "Failed to parse reading: %s", reading), exception);
    }
  }

  protected TupleInput(String[] readings) {
    this(Long.valueOf(readings[0]), System.currentTimeMillis(), Long.valueOf(readings[1]), Long.valueOf(readings[2]));
  }

  protected TupleInput(long timestamp, long stimulus, long key, long value) {
    this.timestamp = timestamp;
    this.stimulus = stimulus;
    this.key = key;
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
    return timestamp;
  }

  public void setTimestamp(long timestamp) {
    this.timestamp = timestamp;
  }

  // Overrides the getStimulus method from the RichTuple interface
  @Override
  public long getStimulus() {
    return stimulus;
  }

  public void setStimulus(long stimulus) {
    this.stimulus = stimulus;
  }

  public String getKey() {
    return String.valueOf(key);
  }

  public void setKey(long key) {
    this.key = key;
  }

  public long getValue() {
    return value;
  }

  public void setValue(long value) {
    this.value = value;
  }

}