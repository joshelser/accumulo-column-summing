/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package joshelser;

import java.io.IOException;
import java.util.Collection;
import java.util.Iterator;
import java.util.Map;
import java.util.SortedSet;
import java.util.TreeSet;

import org.apache.accumulo.core.client.IteratorSetting;
import org.apache.accumulo.core.data.ByteSequence;
import org.apache.accumulo.core.data.Key;
import org.apache.accumulo.core.data.PartialKey;
import org.apache.accumulo.core.data.Range;
import org.apache.accumulo.core.data.Value;
import org.apache.accumulo.core.iterators.IteratorEnvironment;
import org.apache.accumulo.core.iterators.LongCombiner;
import org.apache.accumulo.core.iterators.LongCombiner.Type;
import org.apache.accumulo.core.iterators.SortedKeyValueIterator;
import org.apache.accumulo.core.iterators.TypedValueCombiner.Encoder;
import org.apache.accumulo.core.iterators.WrappingIterator;
import org.apache.commons.lang.StringUtils;
import org.apache.hadoop.io.Text;
import org.apache.log4j.Logger;

import com.google.common.base.Preconditions;

/**
 * 
 */
public class LimitAndSumColumnFamilyIterator extends WrappingIterator {
  private static final Logger log = Logger.getLogger(LimitAndSumColumnFamilyIterator.class);

  public static final String COLUMNS = "limit.columns";
  public static final String TYPE = "limit.type";

  public static void setColumns(IteratorSetting cfg, Collection<String> columns) {
    Preconditions.checkNotNull(columns);
    final StringBuilder sb = new StringBuilder(64);
    for (String column : columns) {
      if (0 != sb.length()) {
        sb.append(',');
      }
      sb.append(column);
    }
    cfg.addOption(COLUMNS, sb.toString());
  }

  private SortedSet<Text> getColumns(Map<String,String> options) {
    String serializedColumns = options.get(COLUMNS);
    if (null == serializedColumns) {
      throw new IllegalArgumentException("Did not find " + COLUMNS + " in the options map");
    }

    String[] columns = StringUtils.split(serializedColumns, ',');
    TreeSet<Text> sortedColumns = new TreeSet<Text>();
    for (String column : columns) {
      sortedColumns.add(new Text(column));
    }

    return sortedColumns;
  }

  private void setEncoder(Map<String,String> options) {
    String type = options.get(TYPE);
    if (type == null)
      throw new IllegalArgumentException("no type specified");
    switch (Type.valueOf(type)) {
      case VARLEN:
        setEncoder(LongCombiner.VAR_LEN_ENCODER);
        return;
      case FIXEDLEN:
        setEncoder(LongCombiner.FIXED_LEN_ENCODER);
        return;
      case STRING:
        setEncoder(LongCombiner.STRING_ENCODER);
        return;
      default:
        throw new IllegalArgumentException();
    }
  }

  /**
   * Sets the Encoder<V> used to translate Values to V and back.
   * 
   * @param encoder
   */
  protected void setEncoder(Encoder<Long> encoder) {
    this.encoder = encoder;
  }

  /**
   * A convenience method for setting the long encoding type.
   * 
   * @param is
   *          IteratorSetting object to configure.
   * @param type
   *          LongCombiner.Type specifying the encoding type.
   */
  public static void setEncodingType(IteratorSetting is, LongCombiner.Type type) {
    is.addOption(TYPE, type.toString());
  }

  private Encoder<Long> encoder = null;
  private SortedSet<Text> desiredColumns;
  private Long sum;

  private Text rowHolder, colfamHolder;
  private Key topKey;
  private Value topValue;

  // Internal state used during seek() or next()
  private Range currentRange;
  private Collection<ByteSequence> currentColumnFamilies;
  private boolean currentColumnFamiliesInclusive;

  public LimitAndSumColumnFamilyIterator() {
    this.rowHolder = new Text();
    this.colfamHolder = new Text();
    this.sum = 0l;
  }

  @Override
  public SortedKeyValueIterator<Key,Value> deepCopy(IteratorEnvironment env) {
    LimitAndSumColumnFamilyIterator skvi = new LimitAndSumColumnFamilyIterator();
    skvi.desiredColumns = this.desiredColumns;
    skvi.encoder = this.encoder;

    // These are likely to not be used (reseeked immediately after deepCopy), but it's prudent to copy them anyways
    skvi.topKey = this.topKey;
    skvi.sum = this.sum;
    skvi.topValue = this.topValue;

    return skvi;
  }

  @Override
  public void init(SortedKeyValueIterator<Key,Value> source, Map<String,String> options, IteratorEnvironment env) throws IOException {
    super.init(source, options, env);
    desiredColumns = getColumns(options);
    setEncoder(options);
  }

  @Override
  public void seek(Range range, Collection<ByteSequence> columnFamilies, boolean inclusive) throws IOException {
    // Make sure we invalidate our last record
    nextRecordNotFound();

    log.debug("Seeking to " + range);

    getSource().seek(range, columnFamilies, inclusive);
    currentRange = range;
    currentColumnFamilies = columnFamilies;
    currentColumnFamiliesInclusive = inclusive;
    aggregate();
  }

  @Override
  public void next() throws IOException {
    // Make sure we invalidate our last record
    nextRecordNotFound();

    // Catch the case where we have no data and there's nothing to next()
    if (!getSource().hasTop()) {
      return;
    }

    getSource().next();
    aggregate();
  }

  @Override
  public Key getTopKey() {
    return topKey;
  }

  @Override
  public Value getTopValue() {
    return topValue;
  }

  @Override
  public boolean hasTop() {
    // topKey and sum should be held in step -- they are either both null, or both non-null
    if ((null == topKey && null != sum) || (null != topKey && null == sum)) {
      throw new IllegalStateException("topKey and sum in differing states: " + topKey + " => " + sum);
    }
    // thus, we can just look at one to determine if there's a record to return.
    return topKey != null;
  }

  /**
   * Given the current position in the {@link source}, filter to only the columns specified. Sets topKey and topValue to non-null on success
   * 
   * @throws IOException
   */
  protected void aggregate() throws IOException {
    if (!getSource().hasTop()) {
      nextRecordNotFound();
      return;
    }

    while (getSource().hasTop()) {
      final Key currentKey = getSource().getTopKey();
      final Value currentValue = getSource().getTopValue();

      currentKey.getColumnFamily(colfamHolder);
      final Iterator<Text> remainingColumns = desiredColumns.tailSet(colfamHolder).iterator();

      if (desiredColumns.contains(colfamHolder)) {
        // found a column we wanted
        nextRecordFound(currentKey, currentValue);

        // The tailSet returns elements greater than or equal to the provided element
        // need to consume the equality element
        remainingColumns.next();
      }

      Range newRange;
      if (remainingColumns.hasNext()) {
        // Get the row avoiding a new Text
        currentKey.getRow(rowHolder);

        Text nextColumn = remainingColumns.next();
        Key nextColumnKey = new Key(rowHolder, nextColumn);
        newRange = new Range(nextColumnKey, true, currentRange.getEndKey(), currentRange.isEndKeyInclusive());
      } else {
        Key nextRow = currentKey.followingKey(PartialKey.ROW);

        // No more data to read, outside of where we wanted to look
        if (!currentRange.contains(nextRow)) {
          setReturnValue();
          return;
        } else {
          // Our new range starts, at earliest, the next possible row and first column family in our desired set
          // but still ends at the original ending
          newRange = new Range(nextRow, true, currentRange.getEndKey(), currentRange.isEndKeyInclusive());
        }
      }

      log.trace("Seeking to " + newRange);

      if (!getSource().hasTop()) {
        setReturnValue();
        return;
      }

      boolean advancedToDesiredPoint = false;
      // Move down to the next Key
      for (int i = 0; i < 10 && !advancedToDesiredPoint; i++) {
        getSource().next();
        if (getSource().hasTop()) {
          if (newRange.contains(getSource().getTopKey())) {
            advancedToDesiredPoint = true;
          }
        } else {
          setReturnValue();
          return;
        }
      }
      
      if (!advancedToDesiredPoint) {
        log.debug("Seeking to find next desired key: " + newRange);
        getSource().seek(newRange, currentColumnFamilies, currentColumnFamiliesInclusive);
      }
    }

    setReturnValue();
  }

  private void nextRecordNotFound() {
    topKey = null;
    sum = null;
  }

  private void nextRecordFound(Key k, Value v) {
    // For every value that we add to the computed sum, we want to also advance
    // the Key that is returned so that we don't re-process any records
    topKey = k;

    if (null == sum) {
      sum = 0l;
    }

    sum += encoder.decode(v.get());
  }

  private void setReturnValue() {
    if (null != sum) {
      log.debug("Computed a sum of " + sum);
      topValue = new Value(encoder.encode(sum));
    } else {
      topValue = new Value(encoder.encode(0l));
    }
  }
}
