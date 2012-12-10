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

  public QueryWorker(MQuery mquery) {
    this.mquery = mquery;
    hiveConf = new HiveConf(SessionState.class);
  }



  /**
   * @param args
   */
  public static void main(String[] args) {
    // TODO Auto-generated method stub

  }

  @Override
  public void run() {
    // TODO Auto-generated method stub
    QueryStore qs = new QueryStore(hiveConf);

    this.mquery.setStatus(MQuery.Status.RUNNING);
    this.runQuery();

    qs.updateQuery(this.mquery);
  }


  public void runQuery() {

    String name = this.mquery.getName();
    SessionState ss = SessionState.start(hiveConf);

    // expect one return per query
    String queryStr = this.mquery.getQuery();
    String[] queries = queryStr.split(";");
    int querylen = queries.length;

    for (int i=0; i<querylen; i++) {
      String cmd = queries[i];
      String cmd_trimmed = cmd.trim();
      String[] tokens = cmd_trimmed.split("\\s+");

      CommandProcessor proc = CommandProcessorFactory.get(tokens[0], hiveConf);
      CommandProcessorResponse resp ;
      String errMsg = null;
      int errCode = 0;

      if (proc != null) {
        if (proc instanceof Driver) {
          Driver qp = (Driver) proc;
          qp.setTryCount(Integer.MAX_VALUE);

          try {
            resp = qp.run(cmd);
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
        errMsg = name + " query processor was not found for query " + cmd;
        this.mquery.setErrorMsg(errMsg);
        this.mquery.setStatus(MQuery.Status.FAILED);
        // processor was null
        l4j.error(errMsg);
      }
    } // end for

    // l4j.debug(getSessionName() + " state is now READY");
    /*synchronized (runnable) {
      runnable.notifyAll();
    }*/
  }

}
