package org.apache.hadoop.hive.hwi;

import java.io.OutputStreamWriter;
import java.net.HttpURLConnection;
import java.net.URL;
import java.net.URLEncoder;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

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

  private void init() {
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
            long start_time = System.currentTimeMillis();

            resp = qp.run(cmd_trimmed);

            long end_time = System.currentTimeMillis();
            mquery.setTotalTime((int) (end_time - start_time));

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


    qs.updateQuery(mquery);
  }

  protected void running() {
    HiveHistoryViewer hv = null;

    if (historyFile != null) {
      try {
        hv = new HiveHistoryViewer(historyFile);
      } catch (Exception e) {
        l4j.error(e.getMessage());
      }
    }

    if(hv != null){
      l4j.debug("running worker:" + hv.getSessionId());

      for (String taskKey : hv.getTaskInfoMap().keySet()) {
        TaskInfo ti = hv.getTaskInfoMap().get(taskKey);
        for (String tiKey : ti.hm.keySet()) {
          if (tiKey.equalsIgnoreCase("TASK_HADOOP_ID")) {
            String jobId = mquery.getJobId() == null ? "" : mquery.getJobId();
            String tid = ti.hm.get(tiKey);
            if (!jobId.contains(tid)) {
              mquery.setJobId(jobId + tid + ";");
              qs.updateQuery(mquery);
            }
          }
        }
      }
    }
  }

  private void finish() {

    HiveHistoryViewer hv = null;

    if (historyFile != null) {
      try {
        hv = new HiveHistoryViewer(historyFile);
      } catch (Exception e) {
        l4j.error(e.getMessage());
      }
    }

    if (hv != null) {
      l4j.debug("finish worker:" + hv.getSessionId());

      Pattern pattern = Pattern.compile("Map-Reduce Framework.CPU time spent \\(ms\\):(\\d+),");

      int ms = 0;

      for (String taskKey : hv.getTaskInfoMap().keySet()) {
        TaskInfo ti = hv.getTaskInfoMap().get(taskKey);
        for (String tiKey : ti.hm.keySet()) {
          if (tiKey.equalsIgnoreCase("TASK_COUNTERS")) {
            l4j.debug(tiKey + ":" + ti.hm.get(tiKey));

            Matcher matcher = pattern.matcher(ti.hm.get(tiKey));
            if (matcher.find()) {
              try {
                ms += Integer.parseInt(matcher.group(1));
              } catch (NumberFormatException e) {
                l4j.error(matcher.group(1) + " is not int");
              }
            }
          }
        }
      }

      mquery.setCpuTime(ms);
    }

    if (mquery.getErrorCode() == null || mquery.getErrorCode() == 0) {
      mquery.setStatus(MQuery.Status.FINISHED);
    } else {
      mquery.setStatus(MQuery.Status.FAILED);
    }

    this.callback();

    qs.updateQuery(mquery);
  }


  /**
   * when query is finished, callback is invoked.
   */
  private void callback() {
    String callback = this.mquery.getCallback();
    if (callback != null && !"".equals(callback)) {
      try {

        String errorCode = "0";
        if (this.mquery.getErrorCode() != null) {
          errorCode = URLEncoder.encode(this.mquery.getErrorCode().toString(), "UTF-8");
        }

        String errorMsg = "";
        if (this.mquery.getErrorMsg() != null) {
          errorMsg = URLEncoder.encode(this.mquery.getErrorMsg(), "UTF-8");
        }

        String postData =
            "id=" + URLEncoder.encode(this.mquery.getId().toString(), "UTF-8")
            + "&status=" +  URLEncoder.encode(this.mquery.getStatus().toString(), "UTF-8")
            + "&error_code=" + errorCode
            + "&error_msg=" + errorMsg
            + "&result_location=" + URLEncoder.encode(this.mquery.getResultLocation(), "UTF-8")
            + "&result_location_url="
              + URLEncoder.encode("/hwi/query_result.php?action=download&id=" + this.mquery.getId(), "UTF-8")
            ;

        int trycallbacktimes = 0;
        do {
          URL callbackUrl = new URL(callback);

          HttpURLConnection urlConn = (HttpURLConnection) callbackUrl.openConnection();
          urlConn.setDoOutput(true);
          urlConn.connect();

          OutputStreamWriter out = new OutputStreamWriter(urlConn.getOutputStream(), "UTF-8");
          out.write(postData);
          out.close();

          int responseCode = urlConn.getResponseCode();

          if (responseCode == 200) {
              break;
          }
        } while (++trycallbacktimes < 3);


        /*
        l4j.debug(urlConn.getResponseMessage());
        l4j.debug(urlConn.getResponseCode());
        BufferedReader bin = new BufferedReader(new InputStreamReader(urlConn.getInputStream(), "UTF-8"));
        String temp;
        while ((temp = bin.readLine()) != null) {
            System.out.println(temp);
        }
        */

      } catch (Exception e) {
          e.printStackTrace();
      }
    }

    l4j.debug(this.mquery.getName() + " state is now FINISHED");
  }


}
