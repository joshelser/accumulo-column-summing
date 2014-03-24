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
import java.io.File;
import java.io.IOException;
import java.util.Collection;
import java.util.Map.Entry;

import org.apache.accumulo.core.client.BatchScanner;
import org.apache.accumulo.core.client.BatchWriter;
import org.apache.accumulo.core.client.BatchWriterConfig;
import org.apache.accumulo.core.client.Connector;
import org.apache.accumulo.core.client.IteratorSetting;
import org.apache.accumulo.core.client.TableNotFoundException;
import org.apache.accumulo.core.client.ZooKeeperInstance;
import org.apache.accumulo.core.client.admin.TableOperations;
import org.apache.accumulo.core.client.security.tokens.PasswordToken;
import org.apache.accumulo.core.data.Key;
import org.apache.accumulo.core.data.Value;
import org.apache.accumulo.core.iterators.LongCombiner;
import org.apache.accumulo.core.security.Authorizations;
import org.apache.accumulo.minicluster.MiniAccumuloCluster;
import org.apache.accumulo.minicluster.MiniAccumuloConfig;
import org.apache.commons.io.FileUtils;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.WritableUtils;
import org.junit.After;
import org.junit.AfterClass;
import org.junit.Assert;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Rule;
import org.junit.rules.TestName;

public class LimitAndSumColumnsBase {
  private static final String PASSWD = "password";

  @Rule
  public TestName testName = new TestName();
  private String table;
  private ZooKeeperInstance inst;
  private Connector conn;

  private static MiniAccumuloCluster cluster;

  @BeforeClass
  public static void setup() throws Exception {
    File macDir = new File(System.getProperty("user.dir") + "/target/LimitAndSumColumnsTest");
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
  
  protected Connector getConnector() {
    return conn;
  }
  
  protected String getTableName() {
    return table;
  }

  protected BatchScanner getBatchScannerWithIterator(Authorizations auths, int numThreads, Collection<String> columns) throws Exception {
    final BatchScanner bs = conn.createBatchScanner(table, new Authorizations(), numThreads);
    bs.fetchColumnFamily(new Text("count"));

    // Create our iterator to limit the columns server-side
    IteratorSetting setting = new IteratorSetting(50, "limit", LimitAndSumColumnFamilyIterator.class);
    LimitAndSumColumnFamilyIterator.setColumns(setting, columns);
    LimitAndSumColumnFamilyIterator.setEncodingType(setting, LongCombiner.Type.VARLEN);
    bs.addScanIterator(setting);

    return bs;
  }
  
  protected long countSerialized(Iterable<Entry<Key,Value>> data) throws IOException {
    long observedSum = 0;
    long numResults = 0l;
    for (Entry<Key,Value> result : data) {
      numResults++;
      ByteArrayInputStream bais = new ByteArrayInputStream(result.getValue().get());
      observedSum += WritableUtils.readVLong(new DataInputStream(bais));
    }
    
    System.out.println("Number of results to sum: " + numResults);
    
    return observedSum;
  }
}
