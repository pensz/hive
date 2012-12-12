package org.apache.hadoop.hive.hwi;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.hive.conf.HiveConf;
import org.apache.hadoop.hive.hwi.model.MQuery;
import org.apache.hadoop.hive.hwi.model.MQuery.Status;
import org.apache.hadoop.hive.ql.CommandNeedRetryException;
import org.apache.hadoop.hive.ql.Driver;
import org.apache.hadoop.hive.ql.history.HiveHistory.TaskInfo;
import org.apache.hadoop.hive.ql.history.HiveHistoryViewer;
import org.apache.hadoop.hive.ql.processors.CommandProcessor;
import org.apache.hadoop.hive.ql.processors.CommandProcessorFactory;
import org.apache.hadoop.hive.ql.processors.CommandProcessorResponse;
import org.apache.hadoop.hive.ql.session.SessionState;

public class QueryWorker implements Runnable {

  protected static final Log l4j = LogFactory.getLog(QueryWorker.class
      .getName());

  private final MQuery mquery;

  private HiveConf hiveConf;

  private QueryStore qs;

  private String historyFile;

  public QueryWorker(MQuery mquery) {
    this.mquery = mquery;
  }

  @Override
  public void run() {
    init();
    runQuery();
    finish();
  }

  protected Status getStatus() {
    return mquery.getStatus();
  }

  private void init(){
    hiveConf = new HiveConf(SessionState.class);

    SessionState.start(hiveConf);
    historyFile = SessionState.get().getHiveHistory().getHistFileName();

    qs = new QueryStore(hiveConf);
  }

  /**
   * run user input queries
   */
  public void runQuery() {

    mquery.setStatus(MQuery.Status.RUNNING);
    qs.updateQuery(mquery);

    String name = mquery.getName();

    // expect one return per query
    String queryStr = mquery.getQuery();
    String[] queries = queryStr.split(";");
    int querylen = queries.length;

    for (int i = 0; i < querylen; i++) {
      String cmd_trimmed = queries[i].trim();
      String[] tokens = cmd_trimmed.split("\\s+");

      if ("select".equalsIgnoreCase(tokens[0])) {
        cmd_trimmed = "INSERT OVERWRITE DIRECTORY '" + mquery.getResultLocation() + "' "
            + cmd_trimmed;
      }

      CommandProcessor proc = CommandProcessorFactory.get(tokens[0], hiveConf);
      CommandProcessorResponse resp;
      String errMsg = null;
      int errCode = 0;

      if (proc != null) {
        if (proc instanceof Driver) {
          Driver qp = (Driver) proc;
          qp.setTryCount(Integer.MAX_VALUE);

          try {
            resp = qp.run(cmd_trimmed);
            errMsg = resp.getErrorMessage();
            errCode = resp.getResponseCode();
          } catch (CommandNeedRetryException e) {
            // TODO Auto-generated catch block
            e.printStackTrace();
          } finally {
            qp.close();
          }
        } else {
          try {
            String cmd_1 = cmd_trimmed.substring(tokens[0].length()).trim();
            resp = proc.run(cmd_1);
            errMsg = resp.getErrorMessage();
            errCode = resp.getResponseCode();
          } catch (CommandNeedRetryException e) {
            // this should never happen if there is no bug
            l4j.error(name + " Exception when executing", e);
          }
        }

        if (errMsg != null) {
          mquery.setErrorMsg(errMsg);
          mquery.setErrorCode(errCode);
        }

      } else {
        // processor was null
        errMsg = name + " query processor was not found for query " + cmd_trimmed;
        mquery.setErrorMsg(errMsg);
        mquery.setErrorCode(404001);
        l4j.error(errMsg);
      }
    } // end for

    l4j.debug(name + " state is now FINISHED");

    qs.updateQuery(mquery);
  }

  protected void running() {
    if (historyFile == null) {
      return;
    }

    HiveHistoryViewer hv = new HiveHistoryViewer(historyFile);


    l4j.debug("running worker:" + hv.getSessionId());

    for (String taskKey : hv.getTaskInfoMap().keySet()) {
      TaskInfo ti = hv.getTaskInfoMap().get(taskKey);
      for (String tiKey : ti.hm.keySet()) {
        if (tiKey.equalsIgnoreCase("TASK_HADOOP_ID")) {
          String jobId = mquery.getJobId() == null ? "" : mquery.getJobId();
          String tid = ti.hm.get(tiKey);
          if(!jobId.contains(tid)) {
            mquery.setJobId(jobId + ";" + tid);
            qs.updateQuery(mquery);
          }
        }
      }
    }
  }

  private void finish() {

    if(mquery.getErrorCode() == null || mquery.getErrorCode() == 0){
      this.mquery.setStatus(MQuery.Status.FINISHED);
    } else {
      this.mquery.setStatus(MQuery.Status.FAILED);
    }

    HiveHistoryViewer hv = new HiveHistoryViewer(historyFile);

    l4j.debug("finish worker:" + hv.getSessionId());

    for (String taskKey : hv.getTaskInfoMap().keySet()) {
      TaskInfo ti = hv.getTaskInfoMap().get(taskKey);
      for (String tiKey : ti.hm.keySet()) {
        if (tiKey.equalsIgnoreCase("TASK_COUNTERS")) {
          l4j.debug(tiKey + ":" + ti.hm.get(tiKey));
        }
      }
    }

    qs.updateQuery(mquery);
  }
}
