/**
 * Copyright (c) 2015 YCSB contributors. All rights reserved.
 *
 * Licensed under the Apache License, Version 2.0 (the "License"); you
 * may not use this file except in compliance with the License. You
 * may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or
 * implied. See the License for the specific language governing
 * permissions and limitations under the License. See accompanying
 * LICENSE file.
 */

package site.ycsb.db;

import com.aerospike.client.AerospikeException;
import com.aerospike.client.Bin;
import com.aerospike.client.Host;
import com.aerospike.client.Key;
import com.aerospike.client.Record;
import com.aerospike.client.policy.ClientPolicy;
import com.aerospike.client.policy.Policy;
import com.aerospike.client.policy.Priority;
import com.aerospike.client.policy.QueryPolicy;
import com.aerospike.client.policy.ReadModeAP;
import com.aerospike.client.policy.ReadModeSC;
import com.aerospike.client.policy.RecordExistsAction;
import com.aerospike.client.policy.WritePolicy;
import com.aerospike.client.query.RecordSet;
import com.aerospike.client.query.Statement;
import site.ycsb.ByteArrayByteIterator;
import site.ycsb.ByteIterator;
import site.ycsb.DBException;
import site.ycsb.Status;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.Set;
import java.util.Vector;
import java.util.concurrent.ThreadLocalRandom;

/**
 * YCSB binding for <a href="http://www.aerospike.com/">Areospike</a>.
 */
public class AerospikeClient extends site.ycsb.DB {
  private static final String DEFAULT_HOST = "localhost";
  private static final String DEFAULT_PORT = "3000";
  private static final String DEFAULT_TIMEOUT = "10000";
  private static final String DEFAULT_NAMESPACE = "ycsb";

  private String namespace = null;
  private String readSetName = null;

  private com.aerospike.client.AerospikeClient client = null;

  private Policy readPolicy = new Policy();
  private WritePolicy insertPolicy = new WritePolicy();
  private WritePolicy updatePolicy = new WritePolicy();
  private WritePolicy deletePolicy = new WritePolicy();

  private List<Key> readKeys;

  private int totalReadKeys = 0;

  private boolean readKeysFromSource;

  @Override
  public synchronized void init() throws DBException {
    this.insertPolicy.recordExistsAction = RecordExistsAction.CREATE_ONLY;
    this.updatePolicy.recordExistsAction = RecordExistsAction.REPLACE_ONLY;

    Properties props = getProperties();

    this.namespace = props.getProperty("as.namespace", DEFAULT_NAMESPACE);

    this.readSetName = props.getProperty("as.readSetName");

    String host = props.getProperty("as.host", DEFAULT_HOST);
    String user = props.getProperty("as.user");
    String password = props.getProperty("as.password");
    int port = Integer.parseInt(props.getProperty("as.port", DEFAULT_PORT));
    int timeout = Integer.parseInt(props.getProperty("as.timeout", DEFAULT_TIMEOUT));
    this.readKeysFromSource = Boolean.parseBoolean(props.getProperty("as.readKeysFromSource", "false"));
    int totalKeys = Integer.parseInt(props.getProperty("as.readKeysCount", "0"));

    readPolicy.setTimeout(timeout);
    insertPolicy.setTimeout(timeout);
    updatePolicy.setTimeout(timeout);
    deletePolicy.setTimeout(timeout);

    readPolicy.readModeAP = ReadModeAP.ONE;
    readPolicy.readModeSC = ReadModeSC.ALLOW_REPLICA;
    readPolicy.maxRetries = Integer.parseInt(props.getProperty("as.maxRetries", "3"));

    ClientPolicy clientPolicy = new ClientPolicy();

    if (user != null && password != null) {
      clientPolicy.user = user;
      clientPolicy.password = password;
    }

    try {
      String[] hosts = host.split(",");
      client = new com.aerospike.client.AerospikeClient(clientPolicy, Arrays.stream(hosts)
          .map(h -> new Host(h, port)).toArray(Host[]::new));
      if(readKeysFromSource && readKeys == null) {
        readKeys = new ArrayList<>();
        Statement statement = new Statement();
        statement.setNamespace(namespace);
        statement.setSetName(readSetName);
        statement.setBinNames("id");
        QueryPolicy queryPolicy = new QueryPolicy();
        queryPolicy.maxConcurrentNodes = hosts.length;
        queryPolicy.readModeAP = ReadModeAP.ONE;
        queryPolicy.readModeSC = ReadModeSC.ALLOW_REPLICA;
        queryPolicy.includeBinData = false;
        queryPolicy.priority = Priority.LOW;
        queryPolicy.maxRetries = 3;
        queryPolicy.sleepBetweenRetries = 1000;
        RecordSet result = client.query(queryPolicy, statement);
        try {
          int i = 0;
          while (result.next()) {
            Key key = result.getKey();
            readKeys.add(key);
            i = i+ 1;
            if(i > totalKeys) {
              break;
            }
          }
          totalReadKeys = readKeys.size();
        } finally {
          result.close();
        }
      }
      System.err.println("Total keys read: " +totalReadKeys);
    } catch (AerospikeException e) {
      throw new DBException(String.format("Error while creating Aerospike " +
          "client for %s:%d.", host, port), e);
    }
  }

  @Override
  public void cleanup() throws DBException {
    client.close();
  }

  @Override
  public Status read(String table, String key, Set<String> fields,
      Map<String, ByteIterator> result) {
    Key newKey;
    if(readKeysFromSource) {
      newKey = getRandomKey();
    } else {
      newKey = new Key(namespace, readSetName, key);
    }
    try {
      Record record;
      if (fields != null) {
        record = client.get(readPolicy, newKey,
            fields.toArray(new String[fields.size()]));
      } else {
        record = client.get(readPolicy, newKey);
      }
      if (record == null) {
        System.err.println("Record key " + newKey + " not found (read)");
        return Status.ERROR;
      }

      for (Map.Entry<String, Object> entry: record.bins.entrySet()) {
        if(entry.getValue() instanceof String) {
          result.put(entry.getKey(), new ByteArrayByteIterator(((String)entry.getValue()).getBytes()));
        }
      }

      return Status.OK;
    } catch (AerospikeException e) {
      System.err.println("Error while reading key " + newKey + ": " + e);
      return Status.ERROR;
    }
  }

  public Key getRandomKey() {
    int randomElementIndex
        = ThreadLocalRandom.current().nextInt(totalReadKeys) % readKeys.size();
    return readKeys.get(randomElementIndex);
  }

  @Override
  public Status scan(String table, String start, int count, Set<String> fields,
      Vector<HashMap<String, ByteIterator>> result) {
    System.err.println("Scan not implemented");
    return Status.ERROR;
  }

  private Status write(String table, String key, WritePolicy writePolicy,
      Map<String, ByteIterator> values) {
    Bin[] bins = new Bin[values.size()];
    int index = 0;

    for (Map.Entry<String, ByteIterator> entry: values.entrySet()) {
      bins[index] = new Bin(entry.getKey(), entry.getValue().toArray());
      ++index;
    }

    Key keyObj = new Key(namespace, table, key);

    try {
      client.put(writePolicy, keyObj, bins);
      return Status.OK;
    } catch (AerospikeException e) {
      System.err.println("Error while writing key " + key + ": " + e);
      return Status.ERROR;
    }
  }

  @Override
  public Status update(String table, String key,
                       Map<String, ByteIterator> values) {
    return write(table, key, updatePolicy, values);
  }

  @Override
  public Status insert(String table, String key,
                       Map<String, ByteIterator> values) {
    return write(table, key, insertPolicy, values);
  }

  @Override
  public Status delete(String table, String key) {
    try {
      if (!client.delete(deletePolicy, new Key(namespace, table, key))) {
        System.err.println("Record key " + key + " not found (delete)");
        return Status.ERROR;
      }

      return Status.OK;
    } catch (AerospikeException e) {
      System.err.println("Error while deleting key " + key + ": " + e);
      return Status.ERROR;
    }
  }
}
