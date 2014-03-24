import java.io.ByteArrayInputStream;
import java.io.DataInputStream;
import java.io.File;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.Map.Entry;
import java.util.Random;
import java.util.TreeSet;

import org.apache.accumulo.core.client.BatchScanner;
import org.apache.accumulo.core.client.BatchWriter;
import org.apache.accumulo.core.client.BatchWriterConfig;
import org.apache.accumulo.core.client.Connector;
import org.apache.accumulo.core.client.IteratorSetting;
import org.apache.accumulo.core.client.Scanner;
import org.apache.accumulo.core.client.TableNotFoundException;
import org.apache.accumulo.core.client.ZooKeeperInstance;
import org.apache.accumulo.core.client.admin.TableOperations;
import org.apache.accumulo.core.client.security.tokens.PasswordToken;
import org.apache.accumulo.core.data.Key;
import org.apache.accumulo.core.data.Mutation;
import org.apache.accumulo.core.data.Range;
import org.apache.accumulo.core.data.Value;
import org.apache.accumulo.core.iterators.LongCombiner;
import org.apache.accumulo.core.iterators.TypedValueCombiner.Encoder;
import org.apache.accumulo.core.security.Authorizations;
import org.apache.accumulo.minicluster.MiniAccumuloCluster;
import org.apache.accumulo.minicluster.MiniAccumuloConfig;
import org.apache.commons.io.FileUtils;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.WritableUtils;
import org.apache.log4j.Logger;
import org.junit.After;
import org.junit.AfterClass;
import org.junit.Assert;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TestName;

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

/**
 * 
 */
public class SumColumnsTest {
  private static final Logger log = Logger.getLogger(SumColumnsTest.class);
  private static final String PASSWD = "password";
  
  @Rule
  public TestName testName = new TestName();
  private String table;
  private ZooKeeperInstance inst;
  private Connector conn;
  
  private static MiniAccumuloCluster cluster;

  @BeforeClass
  public static void setup() throws Exception {
    File macDir = new File(System.getProperty("user.dir") + "/target/SumColumnsTest");
    if (macDir.exists() && macDir.isDirectory()) {
      FileUtils.deleteQuietly(macDir);
    }
    Assert.assertTrue(macDir.mkdirs());
    MiniAccumuloConfig cfg = new MiniAccumuloConfig(macDir, PASSWD);
    cfg.setNumTservers(2);
    cluster = new MiniAccumuloCluster(cfg);
    cluster.start();
  }

  @AfterClass
  public static void stop() throws Exception {
    if (null != cluster) {
      cluster.stop();
    }
  }
  
  @Before
  public void setupTest() throws Exception {
    inst = new ZooKeeperInstance(cluster.getInstanceName(), cluster.getZooKeepers());
    conn = inst.getConnector("root", new PasswordToken(PASSWD));

    table = testName.getMethodName();
    TableOperations tops = conn.tableOperations();
    tops.create(table);
    
    TreeSet<Text> splits = new TreeSet<Text>();
    for (int i = 0; i < 20; i++) {
      splits.add(new Text(Integer.toString(i)));
    }
    tops.addSplits(table, splits);
  }
  
  public BatchWriter getBatchWriter() throws Exception {
    return conn.createBatchWriter(table, new BatchWriterConfig());
  }
  
  @After
  public void removeTable() throws Exception {
    try {
      conn.tableOperations().delete(table);
    } catch (TableNotFoundException tnfe) {
      
    }
  }
  
  private BatchScanner getBatchScanner(Authorizations auths, int numThreads, Collection<String> columns) throws Exception {
    final BatchScanner bs = conn.createBatchScanner(table, new Authorizations(), numThreads);

    // Create our iterator to limit the columns server-side
    IteratorSetting setting = new IteratorSetting(50, "limit", LimitAndSumColumnsIterator.class);
    LimitAndSumColumnsIterator.setColumns(setting, columns);
    LimitAndSumColumnsIterator.setEncodingType(setting, LongCombiner.Type.VARLEN);
    bs.addScanIterator(setting);
    
    return bs;
  }
  
  @Test
  public void test() throws Exception {
    final Encoder<Long> encoder = LongCombiner.VAR_LEN_ENCODER;
    final Random r = new Random();

    ArrayList<String> extraColumns = new ArrayList<String>();
    extraColumns.add("foo1");
    extraColumns.add("bar1");
    extraColumns.add("foobar1");
    
    String countColumn = "count";
    BatchWriter bw = conn.createBatchWriter(table, new BatchWriterConfig());
    
    int numRows = 5;
    ArrayList<Integer> counts = new ArrayList<Integer>(numRows);
    int sum = 0;
    for (int i = 0; i < numRows; i++) {
      int countInRow = r.nextInt(10) + 1;
      sum += countInRow;
      counts.add(countInRow);
      
      Mutation m = new Mutation(String.format("%04d", i));
      for (String extraColumn : extraColumns) {
        m.put(extraColumn, "", extraColumn);
      }
      
      byte[] bytes = encoder.encode(new Long(countInRow));
      m.put(countColumn, "", new Value(bytes));
      
      bw.addMutation(m);
    }
    
    bw.close();
    
    Scanner s = conn.createScanner(table, new Authorizations());
    for (Entry<Key,Value> entry : s) {
      log.info(entry);
    }

    BatchScanner bs = getBatchScanner(new Authorizations(), 2, Collections.singleton(countColumn));
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

}
