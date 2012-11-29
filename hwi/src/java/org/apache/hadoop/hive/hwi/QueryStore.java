package org.apache.hadoop.hive.hwi;

import java.util.Properties;

import javax.jdo.JDOHelper;
import javax.jdo.PersistenceManager;
import javax.jdo.PersistenceManagerFactory;
import javax.jdo.Transaction;

import org.apache.hadoop.hive.hwi.model.MQuery;

public class QueryStore {

  public static void main(String[] args) {
    try {
      Properties properties = new Properties();
      properties.put("com.sun.jdori.option.ConnectionCreate", "true");
      PersistenceManagerFactory pmf =
              JDOHelper.getPersistenceManagerFactory(properties);
      PersistenceManager pm = pmf.getPersistenceManager();
      Transaction tx = pm.currentTransaction();
      tx.begin();
      MQuery mquery = new MQuery();
      mquery.setName("xxx");
      pm.makePersistent(mquery);
      tx.commit();
    } catch (Exception e) {
        System.out.println("Problem creating database");
        e.printStackTrace();
    }

  }

}
