package com.basho.proserv.documentstore.functional;

import java.io.IOException;
import java.io.InputStream;
import java.util.Properties;

import static org.junit.Assert.fail;

public class RiakServerHelper
{
  private static RiakServerHelper singleton = new RiakServerHelper();

  private static int WAIT_MILLIS = 10000; // Time to give Riak to fully come up
  private String binaryLocation;
  private String host;
  private int port;
  private boolean isRunning = false;

  private RiakServerHelper()
  {
    Properties prop = new Properties();
    try (InputStream stream = RiakServerHelper.class.getResourceAsStream("/riakServer.properties"))
    {
      prop.load(stream);
      binaryLocation = prop.getProperty("riakServer.binary","riak");
      host = prop.getProperty("riakServer.host","127.0.0.1");
      port = Integer.parseInt(prop.getProperty("riakServer.port", "8087"));
    }
    catch (IOException e)
    {
      fail("Error loading riakServer.properties " + e);
    }
  }

  public static RiakServerHelper getInstance()
  {
    return singleton;
  }

  public static void ensureRunning() throws IOException, InterruptedException
  {
    if(!getInstance().isRunning)
    {
//      getInstance().start();
    }
  }

  public synchronized void start() throws IOException, InterruptedException
  {
    checkStatus();
    if(!isRunning)
    {
      System.out.println("Starting Riak...");
      Process p = Runtime.getRuntime().exec(binaryLocation + " start");
      p.waitFor();
      Thread.sleep(WAIT_MILLIS); // need to give Riak some extra time to come up.
    }
    isRunning = true;
  }

  public synchronized void stop() throws IOException, InterruptedException
  {
    System.out.println("Stopping Riak...");
    Process p = Runtime.getRuntime().exec(binaryLocation + " stop");
    p.waitFor();
    isRunning = false;
  }

  public synchronized void restart() throws IOException, InterruptedException
  {
    System.out.println("Restarting Riak...");
    Process p = Runtime.getRuntime().exec(binaryLocation + " restart");
    p.waitFor();
    Thread.sleep(WAIT_MILLIS); // need to give Riak some extra time to come up.
    isRunning = true;
  }

  private synchronized void checkStatus() throws IOException, InterruptedException
  {
    Process p = Runtime.getRuntime().exec(binaryLocation + " ping");
    p.waitFor();
    if(p.exitValue() == 0)
    {
      isRunning = true;
    }
  }

  public static String getHost()
  {
    return getInstance().host;
  }

  public static int getPort()
  {
    return getInstance().port;
  }
}
