package org.apache.hadoop.hive.hwi;

import org.apache.hadoop.hive.conf.HiveConf;

public class HWIUtil {

  /*
   * mapred.job.tracker could be host:port or just local
   * mapred.job.tracker.http.address could be host:port or just host. In some
   * configurations http.address is set to 0.0.0.0 we are combining the two
   * variables to provide a url to the job tracker WUI if it exists. If hadoop
   * chose the first available port for the JobTracker, HTTP port will can not
   * determine it.
   */
  public static String getJobTrackerURL(HiveConf conf, String jobid){
    String jt = conf.get("mapred.job.tracker");
    String jth = conf.get("mapred.job.tracker.http.address");
    String[] jtparts = null;
    String[] jthttpParts = null;
    if (jt.equalsIgnoreCase("local")) {
      jtparts = new String[2];
      jtparts[0] = "local";
      jtparts[1] = "";
    } else {
      jtparts = jt.split(":");
    }
    if (jth.contains(":")) {
      jthttpParts = jth.split(":");
    } else {
      jthttpParts = new String[2];
      jthttpParts[0] = jth;
      jthttpParts[1] = "";
    }
    return "http://" + jtparts[0] + ":" + jthttpParts[1] + "/jobdetails.jsp?jobid=" + jobid
        + "&refresh=30";
  }
}
