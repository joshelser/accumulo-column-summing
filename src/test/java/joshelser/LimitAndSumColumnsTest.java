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

import java.io.ByteArrayInputStream;
import java.io.DataInputStream;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map.Entry;
import java.util.Random;
import java.util.concurrent.TimeUnit;

import org.apache.accumulo.core.client.BatchScanner;
import org.apache.accumulo.core.client.BatchWriter;
import org.apache.accumulo.core.client.IteratorSetting;
import org.apache.accumulo.core.client.IteratorSetting.Column;
import org.apache.accumulo.core.conf.Property;
import org.apache.accumulo.core.data.Key;
import org.apache.accumulo.core.data.Mutation;
import org.apache.accumulo.core.data.Range;
import org.apache.accumulo.core.data.Value;
import org.apache.accumulo.core.iterators.LongCombiner;
import org.apache.accumulo.core.iterators.TypedValueCombiner.Encoder;
import org.apache.accumulo.core.iterators.user.SummingCombiner;
import org.apache.accumulo.core.security.Authorizations;
import org.apache.accumulo.core.util.UtilWaitThread;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.WritableUtils;
import org.junit.Assert;
import org.junit.Test;

import com.google.common.base.Stopwatch;

/**
 * 
 */
public class LimitAndSumColumnsTest extends LimitAndSumColumnsBase {
  private static final Encoder<Long> ENCODER = LongCombiner.VAR_LEN_ENCODER;

  @Test
  public void simpleTestManualVerify() throws Exception {
    final Random r = new Random();

    ArrayList<String> extraColumns = new ArrayList<String>();
    extraColumns.add("foo1");
    extraColumns.add("bar1");
    extraColumns.add("foobar1");

    String countColumn = "count";
    BatchWriter bw = getBatchWriter();

    int numRows = 5;
    int sum = 0;
    for (int i = 0; i < numRows; i++) {
      int countInRow = r.nextInt(10) + 1;
      sum += countInRow;

      Mutation m = new Mutation(String.format("%04d", i));
      for (String extraColumn : extraColumns) {
        m.put(extraColumn, "", extraColumn);
      }

      byte[] bytes = ENCODER.encode(new Long(countInRow));
      m.put(countColumn, "", new Value(bytes));

      bw.addMutation(m);
    }

    bw.close();

    BatchScanner bs = getBatchScannerWithIterator(new Authorizations(), 2, Collections.singleton(countColumn));
    bs.setRanges(Collections.singleton(new Range()));
    long observedSum = 0;
    try {
      for (Entry<Key,Value> result : bs) {
        ByteArrayInputStream bais = new ByteArrayInputStream(result.getValue().get());
        observedSum += WritableUtils.readVLong(new DataInputStream(bais));
      }
    } finally {
      bs.close();
    }

    Assert.assertEquals(sum, observedSum);
  }

  @Test
  public void simpleTestCombinerVerify() throws Exception {
    ArrayList<String> extraColumns = new ArrayList<String>();
    extraColumns.add("foo1");
    extraColumns.add("bar1");
    extraColumns.add("foobar1");

    String countColumn = "count";

    long sum = createTestData(5, extraColumns, countColumn);

    BatchScanner bs = getConnector().createBatchScanner(getTableName(), new Authorizations(), 2);
    bs.setRanges(Collections.singleton(new Range()));
    bs.fetchColumnFamily(new Text("count"));
    IteratorSetting combiner = new IteratorSetting(50, "combiner", SummingCombiner.class);
    LongCombiner.setEncodingType(combiner, LongCombiner.Type.VARLEN);
    LongCombiner.setColumns(combiner, Collections.singletonList(new Column(new Text("count"))));
    bs.addScanIterator(combiner);

    long combinerSum = 0l;
    try {
      combinerSum = countSerialized(bs);
    } finally {
      bs.close();
    }

    Assert.assertEquals("Observed sum using LongCombiner did not equal expected sum from LimitAndSumColumnsBase", sum, combinerSum);
  }

  @Test
  public void longTest() throws Exception {
    ArrayList<String> extraColumns = new ArrayList<String>();
    extraColumns.add("foo1");
    extraColumns.add("bar1");
    extraColumns.add("foobar1");
    extraColumns.add("foo2");
    extraColumns.add("bar2");
    extraColumns.add("foobar2");

    String countColumn = "count";

    long sum = createTestData(1000000, extraColumns, countColumn, "64K");

    getConnector().tableOperations().compact(getTableName(), null, null, true, true);
    
    System.out.println("Number of splits for table: " + getConnector().tableOperations().listSplits(getTableName()).size());

    System.out.println("Sleeping before running queries");
    UtilWaitThread.sleep(15000);
    
    for (int i = 0; i < 5; i++) {
      BatchScanner bs = getBatchScannerWithIterator(new Authorizations(), 2, Collections.singleton(countColumn));
      bs.setRanges(Collections.singleton(new Range()));
      long observedSum;
      Stopwatch sw = Stopwatch.createStarted();
      try {
        observedSum = countSerialized(bs);
      } finally {
        bs.close();
      }
      sw.stop();

      System.out.println("Time for iterator: " + sw.elapsed(TimeUnit.MILLISECONDS) + " ms");

      sw.reset();

      Assert.assertEquals("Observed sum with LimitAndSumColumnsBase did not equal expected sum", sum, observedSum);

      bs = getConnector().createBatchScanner(getTableName(), new Authorizations(), 2);
      bs.setRanges(Collections.singleton(new Range()));
      bs.fetchColumnFamily(new Text("count"));
      IteratorSetting combiner = new IteratorSetting(50, "combiner", SummingCombiner.class);
      LongCombiner.setEncodingType(combiner, LongCombiner.Type.VARLEN);
      LongCombiner.setColumns(combiner, Collections.singletonList(new Column(new Text("count"))));
      bs.addScanIterator(combiner);

      long combinerSum = 0l;
      sw.start();
      try {
        combinerSum = countSerialized(bs);
      } finally {
        bs.close();
      }

      sw.stop();

      System.out.println("Time for combiner: " + sw.elapsed(TimeUnit.MILLISECONDS) + " ms");

      Assert.assertEquals("Observed sum using LongCombiner did not equal expected sum from LimitAndSumColumnsBase", observedSum, combinerSum);

      System.out.println();
    }
  }
  
  protected long createTestData(long numRows, List<String> extraColumns, String countColumn) throws Exception {
    return createTestData(numRows, extraColumns, countColumn, Property.TABLE_SPLIT_THRESHOLD.getDefaultValue());
  }

  protected long createTestData(long numRows, List<String> extraColumns, String countColumn, String splitThreshold) throws Exception {
    Random r = new Random();
    BatchWriter bw = getBatchWriter();
    
    getConnector().tableOperations().setProperty(getTableName(), Property.TABLE_SPLIT_THRESHOLD.getKey(), splitThreshold);

    long sum = 0;
    for (long i = 0; i < numRows; i++) {
      int countInRow = r.nextInt(10) + 1;
      sum += countInRow;

      Mutation m = new Mutation(String.format("%08d", i));
      for (String extraColumn : extraColumns) {
        m.put(extraColumn, "", extraColumn);
      }

      byte[] bytes = ENCODER.encode(new Long(countInRow));
      m.put(countColumn, "", new Value(bytes));

      bw.addMutation(m);
    }

    bw.close();

    return sum;
  }
}
