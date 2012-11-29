package org.apache.hadoop.hive.hwi;

import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.util.Properties;

import org.apache.hadoop.hive.shims.JettyShims;
import org.apache.hadoop.hive.shims.ShimLoader;

public class TestRunHWIServer {

  /**
   * @param args
   * @throws Exception
   */
  public static void main(String[] args) throws Exception {

    StringBuilder warFile = new StringBuilder("build/hwi/hive-hwi-");
    Properties props = new Properties();

    // try retrieve version from build.properties file
    try {
      props.load(new FileInputStream("build.properties"));
      warFile.append(props.getProperty("version")).append(".war");
    } catch (FileNotFoundException e) {
      e.printStackTrace();
    } catch (IOException e) {
      e.printStackTrace();
    }

    JettyShims.Server webServer;
    webServer = ShimLoader.getJettyShims().startServer("0.0.0.0", 9999);

    System.out.println("use war file: " + warFile.toString());
    webServer.addWar(warFile.toString(), "/hwi");

    webServer.start();
    webServer.join();

  }

}
