package org.apache.hadoop.hive.hwi;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.hive.conf.HiveConf;
import org.apache.hadoop.hive.hwi.model.MQuery;
import org.apache.hadoop.hive.ql.CommandNeedRetryException;
import org.apache.hadoop.hive.ql.Driver;
import org.apache.hadoop.hive.ql.processors.CommandProcessor;
import org.apache.hadoop.hive.ql.processors.CommandProcessorFactory;
import org.apache.hadoop.hive.ql.processors.CommandProcessorResponse;
import org.apache.hadoop.hive.ql.session.SessionState;

public class QueryWorker implements Runnable {

  protected static final Log l4j = LogFactory.getLog(HWISessionItem.class
      .getName());

  private final MQuery mquery;

  private final HiveConf hiveConf;

  private SessionState sessionState;

  public QueryWorker(MQuery mquery) {
    this.mquery = mquery;
    hiveConf = new HiveConf(SessionState.class);
  }



  /**
   * @param args
   */
  public static void main(String[] args) {

  }

  @Override
  public void run() {
    QueryStore qs = new QueryStore(hiveConf);

    // first, update status to running
    this.mquery.setStatus(MQuery.Status.RUNNING);
    qs.updateQuery(this.mquery);

    // run query and update it.
    this.sessionState = SessionState.start(hiveConf);
    this.runQuery();
    qs.updateQuery(this.mquery);
  }


  /**
   * run user input queries
   */
  public void runQuery() {

    String name = this.mquery.getName();

    // expect one return per query
    String queryStr = this.mquery.getQuery();
    String[] queries = queryStr.split(";");
    int querylen = queries.length;

    for (int i=0; i<querylen; i++) {
      String cmd_trimmed = queries[i].trim();
      String[] tokens = cmd_trimmed.split("\\s+");

      if ("select".equalsIgnoreCase(tokens[0])) {
        cmd_trimmed = "INSERT OVERWRITE DIRECTORY '" + this.mquery.getResultLocation() + "' " + cmd_trimmed;
      }

      CommandProcessor proc = CommandProcessorFactory.get(tokens[0], hiveConf);
      CommandProcessorResponse resp ;
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
          this.mquery.setErrorMsg(errMsg);
          this.mquery.setErrorCode(errCode);
          this.mquery.setStatus(MQuery.Status.FAILED);
        } else {
          this.mquery.setStatus(MQuery.Status.FINISHED);
        }

      } else {
        errMsg = name + " query processor was not found for query " + cmd_trimmed;
        this.mquery.setErrorMsg(errMsg);
        this.mquery.setStatus(MQuery.Status.FAILED);
        // processor was null
        l4j.error(errMsg);
      }
    } // end for

    l4j.debug(name + " state is now FINISHED");
  }

}
