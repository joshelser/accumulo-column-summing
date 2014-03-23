import java.io.ByteArrayOutputStream;
import java.io.DataOutputStream;
import java.io.File;
import java.util.ArrayList;
import java.util.Random;
import java.util.TreeSet;

import org.apache.accumulo.core.client.BatchWriter;
import org.apache.accumulo.core.client.BatchWriterConfig;
import org.apache.accumulo.core.client.Connector;
import org.apache.accumulo.core.client.IteratorSetting;
import org.apache.accumulo.core.client.ZooKeeperInstance;
import org.apache.accumulo.core.client.admin.TableOperations;
import org.apache.accumulo.core.client.security.tokens.PasswordToken;
import org.apache.accumulo.core.data.Mutation;
import org.apache.accumulo.core.iterators.user.SummingCombiner;
import org.apache.accumulo.minicluster.MiniAccumuloCluster;
import org.apache.accumulo.minicluster.MiniAccumuloConfig;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.WritableUtils;
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
  private static final String PASSWD = "password";
  
  @Rule
  public TestName testName = new TestName();
  private String table;
  
  private static MiniAccumuloCluster cluster;

  @BeforeClass
  public static void setup() throws Exception {
    File macDir = new File(System.getProperty("user.dir") + "/target/SumColumnsTest");
    Assert.assertTrue(macDir.mkdirs());
    MiniAccumuloConfig cfg = new MiniAccumuloConfig(macDir, PASSWD);
    cfg.setNumTservers(2);
    cluster = new MiniAccumuloCluster(cfg);
    cluster.start();
  }

  @AfterClass
  public static void stop() throws Exception {
    cluster.stop();
  }
  
  public Connector getConnector() throws Exception {
    return (new ZooKeeperInstance(cluster.getInstanceName(), cluster.getZooKeepers())).getConnector("root", new PasswordToken(PASSWD));
  }
  
  @Before
  public void setupTableForTest() throws Exception {
    table = testName.getMethodName();
    TableOperations tops = getConnector().tableOperations();
    tops.create(table);

    // Create our iterator to limit the columns server-side
    IteratorSetting setting = new IteratorSetting(50, "limit", LimitColumnsIterator.class);
    tops.attachIterator(table, setting);
    
    // Combine them all together
    IteratorSetting combiner = new IteratorSetting(100, "combiner", SummingCombiner.class);
    tops.attachIterator(table, combiner);
    
    TreeSet<Text> splits = new TreeSet<Text>();
    for (int i = 0; i < 20; i++) {
      splits.add(new Text(Integer.toString(i)));
    }
    tops.addSplits(table, splits);
  }
  
  @After
  public void removeTable() throws Exception {
    getConnector().tableOperations().delete(table);
  }

  @Test
  public void test() throws Exception {
    Random r = new Random();

    Connector conn = getConnector();
    
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
      
      ByteArrayOutputStream baos = new ByteArrayOutputStream();
      
      WritableUtils.writeVLong(new DataOutputStream(baos), countInRow);
      m.put(countColumn, "", baos.toString());
      
      bw.addMutation(m);
    }
    
    bw.close();
    
  }

}
