/*
 * Copyright 2020 by OLTPBenchmark Project
 *
 * Licensed under the Apache License, Version 2.0 (the "License"); you may not use this file except
 * in compliance with the License. You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software distributed under the License
 * is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express
 * or implied. See the License for the specific language governing permissions and limitations under
 * the License.
 *
 */

package com.oltpbenchmark;

import com.oltpbenchmark.api.TransactionTypes;
import com.oltpbenchmark.types.DatabaseType;
import com.oltpbenchmark.util.ThreadUtil;
import java.sql.Connection;
import java.util.ArrayList;
import java.util.List;
import org.apache.commons.configuration2.XMLConfiguration;

public class WorkloadConfiguration {

  private final List<Phase> phases = new ArrayList<>();
  private DatabaseType databaseType;
  private String benchmarkName;
  private String url;
  private String username;
  private String password;
  private String driverClass;
  private int batchSize;
  private int maxRetries;
  private int randomSeed = -1;
  private double scaleFactor = 1.0;
  private int ycsbkeys = 10;
  private int crossRatio = 0;
  private int mySkewFactor = 0;
  private int keysPerRegion = 1000;
  private double selectivity = -1.0;
  private int terminals;
  private int loaderThreads = ThreadUtil.availableProcessors();
  private XMLConfiguration xmlConfig = null;
  private WorkloadState workloadState;
  private TransactionTypes transTypes = null;
  private int isolationMode = Connection.TRANSACTION_SERIALIZABLE;
  private String dataDir = null;
  private String ddlPath = null;
  private boolean advancedMonitoringEnabled = false;

  /**
   * If true, establish a new connection for each transaction, otherwise use one persistent
   * connection per client session. This is useful to measure the connection overhead.
   */
  private boolean newConnectionPerTxn = false;

  /**
   * If true, attempt to catch connection closed exceptions and reconnect. This allows the benchmark
   * to recover like a typical application would in the case of a replicated cluster
   * primary-secondary failover.
   */
  private boolean reconnectOnConnectionFailure = false;

  public String getBenchmarkName() {
    return benchmarkName;
  }

  public void setBenchmarkName(String benchmarkName) {
    this.benchmarkName = benchmarkName;
  }

  public WorkloadState getWorkloadState() {
    return workloadState;
  }

  public DatabaseType getDatabaseType() {
    return databaseType;
  }

  public void setDatabaseType(DatabaseType databaseType) {
    this.databaseType = databaseType;
  }

  public String getUrl() {
    return url;
  }

  public void setUrl(String url) {
    this.url = url;
  }

  public String getUsername() {
    return username;
  }

  public void setUsername(String username) {
    this.username = username;
  }

  public String getPassword() {
    return password;
  }

  public void setPassword(String password) {
    this.password = password;
  }

  public String getDriverClass() {
    return driverClass;
  }

  public void setDriverClass(String driverClass) {
    this.driverClass = driverClass;
  }

  public int getBatchSize() {
    return batchSize;
  }

  public void setBatchSize(int batchSize) {
    this.batchSize = batchSize;
  }

  public int getMaxRetries() {
    return maxRetries;
  }

  public void setMaxRetries(int maxRetries) {
    this.maxRetries = maxRetries;
  }

  public void setAdvancedMonitoringEnabled(boolean advancedMonitoringEnabled) {
    this.advancedMonitoringEnabled = true;
  }

  public boolean getAdvancedMonitoringEnabled() {
    return this.advancedMonitoringEnabled;
  }

  /**
   * @return @see newConnectionPerTxn member docs for behavior.
   */
  public boolean getNewConnectionPerTxn() {
    return newConnectionPerTxn;
  }

  /**
   * Used by the configuration loader at startup. Changing it any other time is probably
   * dangeroues. @see newConnectionPerTxn member docs for behavior.
   *
   * @param newConnectionPerTxn
   */
  public void setNewConnectionPerTxn(boolean newConnectionPerTxn) {
    this.newConnectionPerTxn = newConnectionPerTxn;
  }

  /**
   * @return @see reconnectOnConnectionFailure member docs for behavior.
   */
  public boolean getReconnectOnConnectionFailure() {
    return reconnectOnConnectionFailure;
  }

  /**
   * Used by the configuration loader at startup. Changing it any other time is probably
   * dangeroues. @see reconnectOnConnectionFailure member docs for behavior.
   *
   * @param reconnectOnConnectionFailure
   */
  public void setReconnectOnConnectionFailure(boolean reconnectOnConnectionFailure) {
    this.reconnectOnConnectionFailure = reconnectOnConnectionFailure;
  }

  /** Initiate a new benchmark and workload state */
  public void initializeState(BenchmarkState benchmarkState) {
    this.workloadState = new WorkloadState(benchmarkState, phases, terminals);
  }

  public void addPhase(
      int id,
      int time,
      int warmup,
      double rate,
      List<Double> weights,
      boolean rateLimited,
      boolean disabled,
      boolean serial,
      boolean timed,
      int active_terminals,
      Phase.Arrival arrival) {
    phases.add(
        new Phase(
            benchmarkName,
            id,
            time,
            warmup,
            rate,
            weights,
            rateLimited,
            disabled,
            serial,
            timed,
            active_terminals,
            arrival));
  }

  /**
   * The number of loader threads that the framework is allowed to use.
   *
   * @return
   */
  public int getLoaderThreads() {
    return this.loaderThreads;
  }

  public void setLoaderThreads(int loaderThreads) {
    this.loaderThreads = loaderThreads;
  }

  public double getSelectivity() {
    return this.selectivity;
  }

  public void setSelectivity(double selectivity) {
    this.selectivity = selectivity;
  }

  /**
   * The random seed for this benchmark
   *
   * @return
   */
  public int getRandomSeed() {
    return this.randomSeed;
  }

  /**
   * Set the random seed for this benchmark
   *
   * @param randomSeed
   */
  public void setRandomSeed(int randomSeed) {
    this.randomSeed = randomSeed;
  }

  /**
   * Return the scale factor of the database size
   *
   * @return
   */
  public double getScaleFactor() {
    return this.scaleFactor;
  }

  public int getCrossRatio() {
    return this.crossRatio;
  }

  public int getMySkewFactor() {
    return this.mySkewFactor;
  }

  public int getKeysPerRegion() {
    return this.keysPerRegion;
  }

  public int getYCSBkeys() {
    return this.ycsbkeys;
  }
  /**
   * Set the scale factor for the database A value of 1 means the default size. A value greater than
   * 1 means the database is larger A value less than 1 means the database is smaller
   *
   * @param scaleFactor
   */
  public void setScaleFactor(double scaleFactor) {
    this.scaleFactor = scaleFactor;
  }

  public void setCrossRatio(int crossRatio) {
    this.crossRatio = crossRatio;
  }

  public void setMySkewFactor(int mySkewFactor) {
    this.mySkewFactor = mySkewFactor;
  }

  public void setKeysPerRegion(int keysPerRegion) {
    this.keysPerRegion = keysPerRegion;
  }

  public void setYCSBkeys(int ycsbkeys) {
    this.ycsbkeys = ycsbkeys;
  }
  /**
   * Return the number of phases specified in the config file
   *
   * @return
   */
  public int getNumberOfPhases() {
    return phases.size();
  }

  /**
   * Return the directory in which we can find the data files (for example, CSV files) for loading
   * the database.
   */
  public String getDataDir() {
    return this.dataDir;
  }

  /**
   * Set the directory in which we can find the data files (for example, CSV files) for loading the
   * database.
   */
  public void setDataDir(String dir) {
    this.dataDir = dir;
  }

  /** Return the path in which we can find the ddl script. */
  public String getDDLPath() {
    return this.ddlPath;
  }

  /** Set the path in which we can find the ddl script. */
  public void setDDLPath(String ddlPath) {
    this.ddlPath = ddlPath;
  }

  /** A utility method that init the phaseIterator and dialectMap */
  public void init() {
    try {
      Class.forName(this.driverClass);
    } catch (ClassNotFoundException ex) {
      throw new RuntimeException("Failed to initialize JDBC driver '" + this.driverClass + "'", ex);
    }
  }

  public int getTerminals() {
    return terminals;
  }

  public void setTerminals(int terminals) {
    this.terminals = terminals;
  }

  public TransactionTypes getTransTypes() {
    return transTypes;
  }

  public void setTransTypes(TransactionTypes transTypes) {
    this.transTypes = transTypes;
  }

  public List<Phase> getPhases() {
    return phases;
  }

  public XMLConfiguration getXmlConfig() {
    return xmlConfig;
  }

  public void setXmlConfig(XMLConfiguration xmlConfig) {
    this.xmlConfig = xmlConfig;
  }

  public int getIsolationMode() {
    return isolationMode;
  }

  public void setIsolationMode(String mode) {
    switch (mode) {
      case "TRANSACTION_SERIALIZABLE":
        this.isolationMode = Connection.TRANSACTION_SERIALIZABLE;
        break;
      case "TRANSACTION_READ_COMMITTED":
        this.isolationMode = Connection.TRANSACTION_READ_COMMITTED;
        break;
      case "TRANSACTION_REPEATABLE_READ":
        this.isolationMode = Connection.TRANSACTION_REPEATABLE_READ;
        break;
      case "TRANSACTION_READ_UNCOMMITTED":
        this.isolationMode = Connection.TRANSACTION_READ_UNCOMMITTED;
        break;
      case "TRANSACTION_NONE":
        this.isolationMode = Connection.TRANSACTION_NONE;
    }
  }

  public String getIsolationString() {
    if (this.isolationMode == Connection.TRANSACTION_SERIALIZABLE) {
      return "TRANSACTION_SERIALIZABLE";
    } else if (this.isolationMode == Connection.TRANSACTION_READ_COMMITTED) {
      return "TRANSACTION_READ_COMMITTED";
    } else if (this.isolationMode == Connection.TRANSACTION_REPEATABLE_READ) {
      return "TRANSACTION_REPEATABLE_READ";
    } else if (this.isolationMode == Connection.TRANSACTION_READ_UNCOMMITTED) {
      return "TRANSACTION_READ_UNCOMMITTED";
    } else if (this.isolationMode == Connection.TRANSACTION_NONE) {
      return "TRANSACTION_NONE";
    } else {
      return "TRANSACTION_SERIALIZABLE";
    }
  }

  @Override
  public String toString() {
    return "WorkloadConfiguration{"
        + "phases="
        + phases
        + ", databaseType="
        + databaseType
        + ", benchmarkName='"
        + benchmarkName
        + '\''
        + ", url='"
        + url
        + '\''
        + ", username='"
        + username
        + '\''
        + ", password='"
        + password
        + '\''
        + ", driverClass='"
        + driverClass
        + '\''
        + ", batchSize="
        + batchSize
        + ", maxRetries="
        + maxRetries
        + ", scaleFactor="
        + scaleFactor
        + ", crossRatio="
        + crossRatio
        + ", mySkewFactor="
        + mySkewFactor
        + ", keysPerRegion="
        + keysPerRegion
        + ", ycsbKeys="
        + ycsbkeys
        + ", selectivity="
        + selectivity
        + ", terminals="
        + terminals
        + ", loaderThreads="
        + loaderThreads
        + ", workloadState="
        + workloadState
        + ", transTypes="
        + transTypes
        + ", isolationMode="
        + isolationMode
        + ", dataDir='"
        + dataDir
        + '\''
        + '}';
  }
}
