/**
 * 
 */
package org.apache.hadoop.mapred;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.DataInputBuffer;
import org.apache.hadoop.io.compress.CompressionCodec;
import org.apache.hadoop.mapred.IFile.IStreamWriter;
import org.apache.hadoop.mapred.IFile.Reader;
import org.apache.hadoop.util.Progressable;

public class Segment<K extends Object, V extends Object> implements
    Comparable<Segment<K, V>> {
  Reader<K, V> reader = null;
  final DataInputBuffer key = new DataInputBuffer();
  final DataInputBuffer value = new DataInputBuffer();
  Configuration conf = null;
  FileSystem fs = null;
  Path file = null;
  boolean preserve = false;
  CompressionCodec codec = null;
  long segmentOffset = 0;
  long segmentLength = -1;
  long segmentRawLenght = -1;
  Counters.Counter mapOutputsCounter = null;

  public Segment(Configuration conf,
      FileSystem fs,
      Path file,
      CompressionCodec codec,
      boolean preserve) throws IOException {
    this(conf, fs, file, codec, preserve, null);
  }

  public Segment(Configuration conf,
      FileSystem fs,
      Path file,
      CompressionCodec codec,
      boolean preserve,
      Counters.Counter mergedMapOutputsCounter) throws IOException {
    this(conf, fs, file, 0, fs.getFileStatus(file).getLen(), -1, codec,
        preserve, mergedMapOutputsCounter);
  }

  public Segment(Configuration conf,
      FileSystem fs,
      Path file,
      long segmentOffset,
      long segmentLength,
      CompressionCodec codec,
      boolean preserve) throws IOException {
    this(conf, fs, file, segmentOffset, segmentLength, -1, codec, preserve,
        null);
  }

  public Segment(Configuration conf,
      FileSystem fs,
      Path file,
      long segmentOffset,
      long segmentLength,
      long segmentRawLength,
      CompressionCodec codec,
      boolean preserve) throws IOException {
    this(conf, fs, file, segmentOffset, segmentLength, segmentRawLength,
        codec, preserve, null);
  }

  public Segment(Configuration conf,
      FileSystem fs,
      Path file,
      long segmentOffset,
      long segmentLength,
      long segmentRawLength,
      CompressionCodec codec,
      boolean preserve,
      Counters.Counter mergedMapOutputsCounter) throws IOException {
    this.conf = conf;
    this.fs = fs;
    this.file = file;
    this.codec = codec;
    this.preserve = preserve;

    this.segmentOffset = segmentOffset;
    this.segmentLength = segmentLength;
    this.segmentRawLenght = segmentRawLength;

    this.mapOutputsCounter = mergedMapOutputsCounter;
  }

  public Segment(Reader<K, V> reader, boolean preserve) {
    this(reader, preserve, null);
  }

  public Segment(Reader<K, V> reader,
      boolean preserve,
      Counters.Counter mapOutputsCounter) {
    this.reader = reader;
    this.preserve = preserve;

    this.segmentLength = reader.getLength();

    this.mapOutputsCounter = mapOutputsCounter;
  }

  public void init(Counters.Counter readsCounter) throws IOException {
    if (reader == null) {
      FSDataInputStream in = fs.open(file);
      in.seek(segmentOffset);
      reader = new Reader<K, V>(conf, in, segmentLength, segmentRawLenght,
          codec, readsCounter);
    }

    if (mapOutputsCounter != null) {
      mapOutputsCounter.increment(1);
    }
  }

  public long getRawLen() {
    return this.segmentRawLenght;
  }

  public boolean inMemory() {
    return fs == null;
  }

  public DataInputBuffer getKey() {
    return key;
  }

  public DataInputBuffer getValue() throws IOException {
    nextRawValue();
    return value;
  }

  public long getLength() {
    return (reader == null) ? segmentLength : reader.getLength();
  }

  public boolean nextRawKey() throws IOException {
    return reader.nextRawKey(key);
  }

  private void nextRawValue() throws IOException {
    reader.nextRawValue(value);
  }

  void closeReader() throws IOException {
    if (reader != null) {
      reader.close();
      reader = null;
    }
  }

  public void close() throws IOException {
    closeReader();
    if (!preserve && fs != null) {
      fs.delete(file, false);
    }
  }

  public long getPosition() throws IOException {
    return reader.getPosition();
  }

  // This method is used by BackupStore to extract the
  // absolute position after a reset
  long getActualPosition() throws IOException {
    return segmentOffset + reader.getPosition();
  }

  Reader<K, V> getReader() {
    return reader;
  }

  // This method is used by BackupStore to reinitialize the
  // reader to start reading from a different segment offset
  void reinitReader(int offset) throws IOException {
    if (!inMemory()) {
      closeReader();
      segmentOffset = offset;
      segmentLength = fs.getFileStatus(file).getLen() - segmentOffset;
      init(null);
    }
  }

  public void writeTo(
      IStreamWriter writer,
      Progressable progressable,
      Configuration conf) throws IOException {
    reader.dumpTo(writer, progressable, conf);
  }

  @Override
  public int compareTo(Segment<K, V> other) {
    if (this.getLength() == other.getLength()) {
      return 0;
    }
    return this.getLength() < other.getLength() ? -1 : 1;
  }
}