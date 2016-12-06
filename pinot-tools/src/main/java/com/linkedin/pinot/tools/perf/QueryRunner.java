/**
 * Copyright (C) 2014-2016 LinkedIn Corp. (pinot-core@linkedin.com)
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *         http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.linkedin.pinot.tools.perf;

import com.linkedin.pinot.tools.AbstractBaseCommand;
import com.linkedin.pinot.tools.Command;
import java.io.BufferedReader;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileReader;
import java.util.List;
import java.util.Random;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;
import org.apache.commons.io.IOUtils;
import org.apache.commons.math.stat.descriptive.DescriptiveStatistics;
import org.json.JSONObject;
import org.kohsuke.args4j.CmdLineParser;
import org.kohsuke.args4j.Option;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


@SuppressWarnings("FieldCanBeLocal")
public class QueryRunner extends AbstractBaseCommand implements Command {
  private static final Logger LOGGER = LoggerFactory.getLogger(QueryRunner.class);
  private static final int MILLIS_PER_SECOND = 1000;

  @Option(name = "-queryFile", required = true, metaVar = "<String>", usage = "Path to query file.")
  private String _queryFile;
  @Option(name = "-mode", required = true, metaVar = "<String>",
      usage = "Mode of query runner (singleThread|multiThreads|targetQPS|increasingQPS).")
  private String _mode;
  @Option(name = "-numThreads", required = false, metaVar = "<int>",
      usage = "Number of threads sending queries for multiThreads, targetQPS and increasingQPS mode.")
  private int _numThreads;
  @Option(name = "-startQPS", required = false, metaVar = "<int>",
      usage = "Start QPS for targetQPS and increasingQPS mode")
  private double _startQPS;
  @Option(name = "-deltaQPS", required = false, metaVar = "<int>", usage = "Delta QPS for increasingQPS mode.")
  private double _deltaQPS;
  @Option(name = "-reportIntervalMs", required = false, metaVar = "<int>",
      usage = "Interval in milliseconds to report simple statistics for multiThreads, targetQPS and increasingQPS mode"
          + " (default 3000).")
  private int _reportIntervalMs = 3000;
  @Option(name = "-numIntervalsToReportClientTimeStatistics", required = false, metaVar = "<int>",
      usage = "Number of report intervals to report detailed client time statistics (default 10).")
  private int _numIntervalsToReportClientTimeStatistics = 10;
  @Option(name = "-numIntervalsToIncreaseQPS", required = false, metaVar = "<int>",
      usage = "Number of report intervals to increase QPS for increasingQPS mode (default 10).")
  private int _numIntervalsToIncreaseQPS = 10;
  @Option(name = "-brokerHost", required = false, metaVar = "<String>", usage = "Broker host name (default localhost).")
  private String _brokerHost = "localhost";
  @Option(name = "-brokerPort", required = false, metaVar = "<int>", usage = "Broker port number (default 8099).")
  private int _brokerPort = 8099;
  @Option(name = "-help", required = false, help = true, aliases = {"-h", "--h", "--help"},
      usage = "Print this message.")
  private boolean _help;

  @Override
  public boolean getHelp() {
    return _help;
  }

  @Override
  public String getName() {
    return getClass().getSimpleName();
  }

  @Override
  public String description() {
    return "Run queries from a query file in singleThread, multiThreads, targetQPS or increasingQPS mode.";
  }

  @Override
  public boolean execute()
      throws Exception {
    if (!new File(_queryFile).isFile()) {
      LOGGER.error("Argument queryFile: {} is not a valid file.", _queryFile);
      printUsage();
      return false;
    }
    if (_numIntervalsToReportClientTimeStatistics <= 0) {
      LOGGER.error("Argument numIntervalsToReportClientTimeStatistics should be a positive number.");
      printUsage();
      return false;
    }

    LOGGER.info("Start query runner targeting broker: {}:{}", _brokerHost, _brokerPort);
    PerfBenchmarkDriverConf conf = new PerfBenchmarkDriverConf();
    conf.setBrokerHost(_brokerHost);
    conf.setBrokerPort(_brokerPort);
    conf.setRunQueries(true);
    conf.setStartZookeeper(false);
    conf.setStartController(false);
    conf.setStartBroker(false);
    conf.setStartServer(false);

    switch (_mode) {
      case "singleThread":
        LOGGER.info("MODE singleThread with queryFile: {}, numIntervalsToReportClientTimeStatistics: {}", _queryFile,
            _numIntervalsToReportClientTimeStatistics);
        singleThreadedQueryRunner(conf, _queryFile, _numIntervalsToReportClientTimeStatistics);
        break;
      case "multiThreads":
        if (_numThreads <= 0) {
          LOGGER.error("For multiThreads mode, argument numThreads should be a positive number.");
          printUsage();
          break;
        }
        if (_reportIntervalMs <= 0) {
          LOGGER.error("For multiThreads mode, argument reportIntervalMs should be a positive number.");
          printUsage();
          break;
        }
        LOGGER.info("MODE multiThreads with queryFile: {}, numThreads: {}, reportIntervalMs: {}, "
                + "numIntervalsToReportClientTimeStatistics: {}", _queryFile, _numThreads, _reportIntervalMs,
            _numIntervalsToReportClientTimeStatistics);
        multiThreadedQueryRunner(conf, _queryFile, _numThreads, _reportIntervalMs,
            _numIntervalsToReportClientTimeStatistics);
        break;
      case "targetQPS":
        if (_numThreads <= 0) {
          LOGGER.error("For targetQPS mode, argument numThreads should be a positive number.");
          printUsage();
          break;
        }
        if (_startQPS <= 0) {
          System.out.println("For targetQPS mode, argument startQPS should be a positive number.");
          printUsage();
          break;
        }
        if (_reportIntervalMs <= 0) {
          LOGGER.error("For targetQPS mode, argument reportIntervalMs should be a positive number.");
          printUsage();
          break;
        }
        LOGGER.info("MODE targetQPS with queryFile: {}, numThreads: {}, startQPS: {}, reportIntervalMs: {}, "
                + "numIntervalsToReportClientTimeStatistics: {}", _queryFile, _numThreads, _startQPS, _reportIntervalMs,
            _numIntervalsToReportClientTimeStatistics);
        targetQPSQueryRunner(conf, _queryFile, _numThreads, _startQPS, _reportIntervalMs,
            _numIntervalsToReportClientTimeStatistics);
        break;
      case "increasingQPS":
        if (_numThreads <= 0) {
          LOGGER.error("For increasingQPS mode, argument numThreads should be a positive number.");
          printUsage();
          break;
        }
        if (_startQPS <= 0) {
          System.out.println("For increasingQPS mode, argument startQPS should be a positive number.");
          printUsage();
          break;
        }
        if (_deltaQPS <= 0) {
          System.out.println("For increasingQPS mode, argument deltaQPS should be a positive number.");
          printUsage();
          break;
        }
        if (_reportIntervalMs <= 0) {
          LOGGER.error("For increasingQPS mode, argument reportIntervalMs should be a positive number.");
          printUsage();
          break;
        }
        if (_numIntervalsToIncreaseQPS <= 0) {
          LOGGER.error("For increasingQPS mode, argument numIntervalsToIncreaseQPS should be a positive number.");
          printUsage();
          break;
        }
        LOGGER.info("MODE increasingQPS with queryFile: {}, numThreads: {}, startQPS: {}, deltaQPS: {}, "
                + "reportIntervalMs: {}, numIntervalsToReportClientTimeStatistics: {}, numIntervalsToIncreaseQPS: {}",
            _queryFile, _numThreads, _startQPS, _deltaQPS, _reportIntervalMs, _numIntervalsToReportClientTimeStatistics,
            _numIntervalsToIncreaseQPS);
        increasingQPSQueryRunner(conf, _queryFile, _numThreads, _startQPS, _deltaQPS, _reportIntervalMs,
            _numIntervalsToReportClientTimeStatistics, _numIntervalsToIncreaseQPS);
        break;
      default:
        LOGGER.error("Invalid mode: {}" + _mode);
        printUsage();
        break;
    }
    return true;
  }

  /**
   * Use single thread to run queries as fast as possible.
   * <p>Use a single thread to send queries back to back and log statistic information periodically.
   * <p>Queries are picked sequentially from the query file.
   * <p>Query runner will stop when all queries in the query file has been executed.
   *
   * @param conf perf benchmark driver config.
   * @param queryFile query file.
   * @param numIntervalsToReportClientTimeStatistics number of intervals to report detailed client time statistics.
   * @throws Exception
   */
  public static void singleThreadedQueryRunner(PerfBenchmarkDriverConf conf, String queryFile,
      int numIntervalsToReportClientTimeStatistics)
      throws Exception {
    PerfBenchmarkDriver driver = new PerfBenchmarkDriver(conf);

    try (BufferedReader bufferedReader = new BufferedReader(new FileReader(queryFile))) {
      int numQueriesExecuted = 0;
      int totalBrokerTime = 0;
      int totalClientTime = 0;
      DescriptiveStatistics clientTimeStatistics = new DescriptiveStatistics();

      long startTime = System.currentTimeMillis();
      String query;
      while ((query = bufferedReader.readLine()) != null) {
        JSONObject response = driver.postQuery(query);
        numQueriesExecuted++;
        long brokerTime = response.getLong("timeUsedMs");
        totalBrokerTime += brokerTime;
        long clientTime = response.getLong("totalTime");
        totalClientTime += clientTime;
        clientTimeStatistics.addValue(clientTime);

        if (numQueriesExecuted % 1000 == 0) {
          long timePassed = System.currentTimeMillis() - startTime;
          LOGGER.info("Time Passed: {}ms, Average QPS: {}, Average Broker Time: {}ms, Average Client Time: {}ms.",
              timePassed, numQueriesExecuted / ((double) timePassed / MILLIS_PER_SECOND),
              totalBrokerTime / (double) numQueriesExecuted, totalClientTime / (double) numQueriesExecuted);
          if (numQueriesExecuted % (1000 * numIntervalsToReportClientTimeStatistics) == 0) {
            reportClientTimeStatistics(clientTimeStatistics);
          }
        }
      }

      long timePassed = System.currentTimeMillis() - startTime;
      LOGGER.info("Time Passed: {}ms, Average QPS: {}, Average Broker Time: {}ms, Average Client Time: {}ms.",
          timePassed, numQueriesExecuted / ((double) timePassed / MILLIS_PER_SECOND),
          totalBrokerTime / (double) numQueriesExecuted, totalClientTime / (double) numQueriesExecuted);
      reportClientTimeStatistics(clientTimeStatistics);
    }
  }

  /**
   * Use multiple threads to run queries as fast as possible.
   * <p>Start <code>numThreads</code> worker threads to send queries (blocking call) back to back, and use the main
   * thread to collect and log statistic information periodically.
   * <p>Queries are picked randomly from the query file.
   * <p>Query runner will run forever.
   *
   * @param conf perf benchmark driver config.
   * @param queryFile query file.
   * @param numThreads number of threads sending queries.
   * @param reportIntervalMs report interval in milliseconds.
   * @param numIntervalsToReportClientTimeStatistics number of intervals to report detailed client time statistics.
   * @throws Exception
   */
  @SuppressWarnings("InfiniteLoopStatement")
  public static void multiThreadedQueryRunner(PerfBenchmarkDriverConf conf, String queryFile, int numThreads,
      int reportIntervalMs, int numIntervalsToReportClientTimeStatistics)
      throws Exception {
    final Random random = new Random();

    final List<String> queries;
    try (FileInputStream input = new FileInputStream(new File(queryFile))) {
      queries = IOUtils.readLines(input);
    }
    final int numQueries = queries.size();

    final PerfBenchmarkDriver driver = new PerfBenchmarkDriver(conf);
    final AtomicInteger numQueriesExecuted = new AtomicInteger(0);
    final AtomicLong totalBrokerTime = new AtomicLong(0L);
    final AtomicLong totalClientTime = new AtomicLong(0L);
    final DescriptiveStatistics clientTimeStatistics = new DescriptiveStatistics();

    ExecutorService executorService = Executors.newFixedThreadPool(numThreads);
    for (int i = 0; i < numThreads; i++) {
      executorService.submit(new Runnable() {
        @Override
        public void run() {
          while (true) {
            String query = queries.get(random.nextInt(numQueries));
            try {
              executeQueryInMultiThreads(driver, query, numQueriesExecuted, totalBrokerTime, totalClientTime,
                  clientTimeStatistics);
            } catch (Exception e) {
              LOGGER.error("Caught exception while running query: {}", query, e);
              return;
            }
          }
        }
      });
    }
    executorService.shutdown();

    long startTime = System.currentTimeMillis();
    int numReportIntervals = 0;
    while (true) {
      if (executorService.isTerminated()) {
        LOGGER.error("All threads got exception and already dead.");
        return;
      }
      Thread.sleep(reportIntervalMs);
      long timePassed = System.currentTimeMillis() - startTime;
      LOGGER.info("Time Passed: {}ms, Average QPS: {}, Average Broker Time: {}ms, Average Client Time: {}ms.",
          timePassed, numQueriesExecuted.get() / ((double) timePassed / MILLIS_PER_SECOND),
          totalBrokerTime.get() / (double) numQueriesExecuted.get(),
          totalClientTime.get() / (double) numQueriesExecuted.get());
      numReportIntervals++;
      if (numReportIntervals == numIntervalsToReportClientTimeStatistics) {
        reportClientTimeStatistics(clientTimeStatistics);
        numReportIntervals = 0;
      }
    }
  }

  /**
   * Use multiple threads to run query at a target QPS.
   * <p>Use a concurrent linked queue to buffer the queries to be sent. Use the main thread to insert queries into the
   * queue at the target QPS, and start <code>numThreads</code> worker threads to fetch queries from the queue and send
   * them.
   * <p>The main thread is responsible for collecting and logging the statistic information periodically.
   * <p>Queries are picked randomly from the query file.
   * <p>Query runner will run forever or until too many queries blocked in the queue.
   *
   * @param conf perf benchmark driver config.
   * @param queryFile query file.
   * @param numThreads number of threads sending queries.
   * @param startQPS start QPS (target QPS).
   * @param reportIntervalMs report interval in milliseconds.
   * @param numIntervalsToReportClientTimeStatistics number of intervals to report detailed client time statistics.
   * @throws Exception
   */
  @SuppressWarnings("InfiniteLoopStatement")
  public static void targetQPSQueryRunner(PerfBenchmarkDriverConf conf, String queryFile, int numThreads,
      double startQPS, int reportIntervalMs, int numIntervalsToReportClientTimeStatistics)
      throws Exception {
    Random random = new Random();

    List<String> queries;
    try (FileInputStream input = new FileInputStream(new File(queryFile))) {
      queries = IOUtils.readLines(input);
    }
    int numQueries = queries.size();

    PerfBenchmarkDriver driver = new PerfBenchmarkDriver(conf);
    AtomicInteger numQueriesExecuted = new AtomicInteger(0);
    AtomicLong totalBrokerTime = new AtomicLong(0L);
    AtomicLong totalClientTime = new AtomicLong(0L);
    DescriptiveStatistics clientTimeStatistics = new DescriptiveStatistics();
    ConcurrentLinkedQueue<String> queryQueue = new ConcurrentLinkedQueue<>();

    ExecutorService executorService = Executors.newFixedThreadPool(numThreads);
    for (int i = 0; i < numThreads; i++) {
      executorService.submit(
          new Worker(driver, numQueriesExecuted, totalBrokerTime, totalClientTime, clientTimeStatistics, queryQueue));
    }
    executorService.shutdown();

    int intervalMs = (int) (MILLIS_PER_SECOND / startQPS);
    int queueLengthThreshold = Math.max(20, (int) startQPS);
    long startTime = System.currentTimeMillis();
    int numReportIntervals = 0;
    while (true) {
      if (executorService.isTerminated()) {
        LOGGER.error("All threads got exception and already dead.");
        return;
      }
      long reportStartTime = System.currentTimeMillis();
      while (System.currentTimeMillis() - reportStartTime <= reportIntervalMs) {
        if (queryQueue.size() > queueLengthThreshold) {
          executorService.shutdownNow();
          LOGGER.error("Cannot achieve target QPS of: " + startQPS);
          return;
        }
        queryQueue.add(queries.get(random.nextInt(numQueries)));
        Thread.sleep(intervalMs);
      }
      long timePassed = System.currentTimeMillis() - startTime;
      LOGGER.info(
          "Target QPS: {}, Time Passed: {}ms, Average QPS: {}, Average Broker Time: {}ms, Average Client Time: {}ms.",
          startQPS, timePassed, numQueriesExecuted.get() / ((double) timePassed / MILLIS_PER_SECOND),
          totalBrokerTime.get() / (double) numQueriesExecuted.get(),
          totalClientTime.get() / (double) numQueriesExecuted.get());
      numReportIntervals++;
      if (numReportIntervals == numIntervalsToReportClientTimeStatistics) {
        reportClientTimeStatistics(clientTimeStatistics);
        numReportIntervals = 0;
      }
    }
  }

  /**
   * Use multiple threads to run query at an increasing target QPS.
   * <p>Use a concurrent linked queue to buffer the queries to be sent. Use the main thread to insert queries into the
   * queue at the target QPS, and start <code>numThreads</code> worker threads to fetch queries from the queue and send
   * them.
   * <p>We start with the start QPS, and keep adding delta QPS to the start QPS during the test.
   * <p>The main thread is responsible for collecting and logging the statistic information periodically.
   * <p>Queries are picked randomly from the query file.
   * <p>Query runner will run until too many queries blocked in the queue.
   *
   * @param conf perf benchmark driver config.
   * @param queryFile query file.
   * @param numThreads number of threads sending queries.
   * @param startQPS start QPS.
   * @param deltaQPS delta QPS.
   * @param reportIntervalMs report interval in milliseconds.
   * @param numIntervalsToReportClientTimeStatistics number of intervals to report detailed client time statistics.
   * @param numIntervalsToIncreaseQPS number of intervals to increase QPS.
   * @throws Exception
   */
  @SuppressWarnings("InfiniteLoopStatement")
  public static void increasingQPSQueryRunner(PerfBenchmarkDriverConf conf, String queryFile, int numThreads,
      double startQPS, double deltaQPS, int reportIntervalMs, int numIntervalsToReportClientTimeStatistics,
      int numIntervalsToIncreaseQPS)
      throws Exception {
    Random random = new Random();

    List<String> queries;
    try (FileInputStream input = new FileInputStream(new File(queryFile))) {
      queries = IOUtils.readLines(input);
    }
    int numQueries = queries.size();

    PerfBenchmarkDriver driver = new PerfBenchmarkDriver(conf);
    AtomicInteger numQueriesExecuted = new AtomicInteger(0);
    AtomicLong totalBrokerTime = new AtomicLong(0L);
    AtomicLong totalClientTime = new AtomicLong(0L);
    DescriptiveStatistics clientTimeStatistics = new DescriptiveStatistics();
    ConcurrentLinkedQueue<String> queryQueue = new ConcurrentLinkedQueue<>();

    ExecutorService executorService = Executors.newFixedThreadPool(numThreads);
    for (int i = 0; i < numThreads; i++) {
      executorService.submit(
          new Worker(driver, numQueriesExecuted, totalBrokerTime, totalClientTime, clientTimeStatistics, queryQueue));
    }
    executorService.shutdown();

    double currentQPS = startQPS;
    int intervalMs = Integer.MAX_VALUE;
    long startTime = System.currentTimeMillis();
    int numReportIntervals = 0;
    while (true) {
      if (executorService.isTerminated()) {
        LOGGER.error("All threads got exception and already dead.");
        return;
      }

      // Find the next interval.
      int newIntervalMs;
      while ((newIntervalMs = (int) (MILLIS_PER_SECOND / currentQPS)) == intervalMs) {
        currentQPS += deltaQPS;
      }
      intervalMs = newIntervalMs;
      int queueLengthThreshold = Math.max(20, (int) currentQPS);

      int numIntervalsForCurrentQPS = 0;
      while (numIntervalsForCurrentQPS != numIntervalsToIncreaseQPS) {
        long reportStartTime = System.currentTimeMillis();
        while (System.currentTimeMillis() - reportStartTime <= reportIntervalMs) {
          if (queryQueue.size() > queueLengthThreshold) {
            executorService.shutdownNow();
            LOGGER.error("Cannot achieve current target QPS of: " + currentQPS);
            return;
          }
          queryQueue.add(queries.get(random.nextInt(numQueries)));
          Thread.sleep(intervalMs);
        }
        long timePassed = System.currentTimeMillis() - startTime;
        LOGGER.info("Numbers in the report are the accumulated results for all target QPS.");
        LOGGER.info(
            "Current Target QPS: {}, Time Passed: {}ms, Average QPS: {}, Average Broker Time: {}ms, Average Client"
                + " Time: {}ms.", currentQPS, timePassed,
            numQueriesExecuted.get() / ((double) timePassed / MILLIS_PER_SECOND),
            totalBrokerTime.get() / (double) numQueriesExecuted.get(),
            totalClientTime.get() / (double) numQueriesExecuted.get());
        numReportIntervals++;
        if (numReportIntervals == numIntervalsToReportClientTimeStatistics) {
          reportClientTimeStatistics(clientTimeStatistics);
          numReportIntervals = 0;
        }
        numIntervalsForCurrentQPS++;
      }
    }
  }

  @SuppressWarnings("SynchronizationOnLocalVariableOrMethodParameter")
  private static void reportClientTimeStatistics(DescriptiveStatistics clientTimeStatistics) {
    synchronized (clientTimeStatistics) {
      LOGGER.info("--------------------------------------------------------------------------------");
      LOGGER.info("CLIENT TIME STATISTICS:");
      LOGGER.info(clientTimeStatistics.toString());
      LOGGER.info("10th percentile: {}ms", clientTimeStatistics.getPercentile(10.0));
      LOGGER.info("25th percentile: {}ms", clientTimeStatistics.getPercentile(25.0));
      LOGGER.info("50th percentile: {}ms", clientTimeStatistics.getPercentile(50.0));
      LOGGER.info("90th percentile: {}ms", clientTimeStatistics.getPercentile(90.0));
      LOGGER.info("95th percentile: {}ms", clientTimeStatistics.getPercentile(95.0));
      LOGGER.info("99th percentile: {}ms", clientTimeStatistics.getPercentile(99.0));
      LOGGER.info("99.9th percentile: {}ms", clientTimeStatistics.getPercentile(99.9));
      LOGGER.info("--------------------------------------------------------------------------------");
    }
  }

  @SuppressWarnings("SynchronizationOnLocalVariableOrMethodParameter")
  private static void executeQueryInMultiThreads(PerfBenchmarkDriver driver, String query,
      AtomicInteger numQueriesExecuted, AtomicLong totalBrokerTime, AtomicLong totalClientTime,
      DescriptiveStatistics clientTimeStatistics)
      throws Exception {
    JSONObject response = driver.postQuery(query);
    numQueriesExecuted.getAndIncrement();
    long brokerTime = response.getLong("timeUsedMs");
    totalBrokerTime.getAndAdd(brokerTime);
    long clientTime = response.getLong("totalTime");
    totalClientTime.getAndAdd(clientTime);
    synchronized (clientTimeStatistics) {
      clientTimeStatistics.addValue(clientTime);
    }
  }

  private static class Worker implements Runnable {
    private final PerfBenchmarkDriver _driver;
    private final AtomicInteger _numQueriesExecuted;
    private final AtomicLong _totalBrokerTime;
    private final AtomicLong _totalClientTime;
    private final DescriptiveStatistics _clientTimeStatistics;
    private final ConcurrentLinkedQueue<String> _queryQueue;

    private Worker(PerfBenchmarkDriver driver, AtomicInteger numQueriesExecuted, AtomicLong totalBrokerTime,
        AtomicLong totalClientTime, DescriptiveStatistics clientTimeStatistics,
        ConcurrentLinkedQueue<String> queryQueue) {
      _driver = driver;
      _numQueriesExecuted = numQueriesExecuted;
      _totalBrokerTime = totalBrokerTime;
      _totalClientTime = totalClientTime;
      _clientTimeStatistics = clientTimeStatistics;
      _queryQueue = queryQueue;
    }

    @Override
    public void run() {
      while (true) {
        String query = _queryQueue.poll();
        if (query == null) {
          try {
            Thread.sleep(1);
            continue;
          } catch (InterruptedException e) {
            LOGGER.error("Caught InterruptedException.", e);
            return;
          }
        }
        try {
          executeQueryInMultiThreads(_driver, query, _numQueriesExecuted, _totalBrokerTime, _totalClientTime,
              _clientTimeStatistics);
        } catch (Exception e) {
          LOGGER.error("Caught exception while running query: {}", query, e);
          return;
        }
      }
    }
  }

  public static void main(String[] args)
      throws Exception {
    QueryRunner queryRunner = new QueryRunner();
    CmdLineParser parser = new CmdLineParser(queryRunner);
    parser.parseArgument(args);

    if (queryRunner._help) {
      queryRunner.printUsage();
    } else {
      queryRunner.execute();
    }
  }
}

