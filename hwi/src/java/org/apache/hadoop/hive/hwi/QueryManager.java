package org.apache.hadoop.hive.hwi;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.concurrent.Executors;
import java.util.concurrent.ThreadPoolExecutor;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.hive.hwi.model.MQuery;
import org.apache.hadoop.hive.hwi.model.MQuery.Status;

public class QueryManager implements Runnable{

  protected static final Log l4j = LogFactory.getLog(QueryManager.class
      .getName());

  private final ThreadPoolExecutor executor;
  private boolean goOn;
  private final ArrayList<QueryWorker> workers;

  protected QueryManager(){
    goOn = true;
    executor = (ThreadPoolExecutor) Executors.newFixedThreadPool(20);
    workers = new ArrayList<QueryWorker>();
  }

  @Override
  public void run() {
    while(goOn) {
      Iterator<QueryWorker> it= workers.iterator();

      while(it.hasNext()){
        QueryWorker worker = it.next();
        Status status = worker.getStatus();
        switch(status){
        case INITED:
          l4j.debug("find inited worker");
          break;
        case RUNNING:
          worker.running();
          break;
        default:
          l4j.debug("remove worker:" + status);
          it.remove();
          break;
        }
      }

      try {
        l4j.debug("go to sleep...");
        Thread.sleep(1000);
      } catch (InterruptedException ex) {
        l4j.error("Could not sleep ", ex);
      }
    }

    l4j.debug("goOn is false. Loop has ended.");

    executor.shutdown();
  }

  public boolean submit(MQuery mquery){
    if(!goOn){
      return false;
    }

    QueryWorker worker = new QueryWorker(mquery);
    workers.add(worker);
    executor.execute(worker);

    return true;
  }

  protected boolean isGoOn() {
    return goOn;
  }

  protected void setGoOn(boolean goOn) {
    this.goOn = goOn;
  }

}
