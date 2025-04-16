package com.vincenzogulisano.usecases.linearroad;

import java.io.Serializable;
import java.util.Objects;
import java.util.regex.Pattern;

import common.tuple.RichTuple;

/**
 * TODO Please notice: the right way would be to define serialyzers, not put
 * transient here, otherwise the tuple might not be usable in other
 * applications.
 */
public class TupleInput implements RichTuple, Serializable {

  private long timestamp;
  private transient long stimulus;
  private transient String key;
  public static final transient Pattern DELIMITER_PATTERN = Pattern.compile(",");
  private transient int type;
  private transient long vid;
  private int speed;
  private transient int xway;
  private transient int lane;
  private transient int dir;
  private transient int seg;
  private transient int pos;

  public long getSize() {
    return 144;
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
    this(Integer
        .valueOf(readings[0]), Long.valueOf(readings[1]),
        Integer.valueOf(readings[2]), Integer
            .valueOf(readings[3]),
        Integer
            .valueOf(readings[4]),
        Integer
            .valueOf(readings[5]),
        Integer
            .valueOf(readings[6]),
        Integer
            .valueOf(readings[7]),
        Integer
            .valueOf(readings[8]),
        System.currentTimeMillis());
  }

  protected TupleInput(int type, long time, int vid, int speed,
      int xway, int lane, int dir, int seg, int pos, long stimulus) {
    this.stimulus = stimulus;
    this.timestamp = time;
    this.key = xway + "-" + vid;
    this.type = type;
    this.vid = vid;
    this.speed = speed;
    this.xway = xway;
    this.lane = lane;
    this.dir = dir;
    this.seg = seg;
    this.pos = pos;
  }

  public int getType() {
    return type;
  }

  public void setType(int type) {
    this.type = type;
  }

  public long getVid() {
    return vid;
  }

  public void setVid(long vid) {
    this.vid = vid;
  }

  public int getSpeed() {
    return speed;
  }

  public void setSpeed(int speed) {
    this.speed = speed;
  }

  public int getXway() {
    return xway;
  }

  public void setXway(int xway) {
    this.xway = xway;
  }

  public int getLane() {
    return lane;
  }

  public void setLane(int lane) {
    this.lane = lane;
  }

  public int getDir() {
    return dir;
  }

  public void setDir(int dir) {
    this.dir = dir;
  }

  public int getSeg() {
    return seg;
  }

  public void setSeg(int seg) {
    this.seg = seg;
  }

  public int getPos() {
    return pos;
  }

  public void setPos(int pos) {
    this.pos = pos;
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
    TupleInput that = (TupleInput) o;
    return type == that.type &&
        vid == that.vid &&
        speed == that.speed &&
        xway == that.xway &&
        lane == that.lane &&
        dir == that.dir &&
        seg == that.seg &&
        pos == that.pos;
  }

  @Override
  public int hashCode() {
    return Objects.hash(super.hashCode(), type, vid, speed, xway, lane, dir, seg, pos);
  }

  @Override
  public String toString() {
    return type + "," + getTimestamp() + "," + vid + "," + speed + ","
        + xway + "," + lane + "," + dir + "," + seg + "," + pos;
  }

  public long getTimestamp() {
    return this.timestamp;
  }

  public java.lang.String getKey() {
    return this.key;
  }

  public long getStimulus() {
    return this.stimulus;
  }

  public void setStimulus(long stimulus) {
    this.stimulus = stimulus;
  }

}