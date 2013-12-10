package jdbc;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;
import java.io.IOException;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.Statement;

import java.text.SimpleDateFormat;

public class GreenplumConnection
{

  public static final String DATE_FORMAT_NOW = "yyyy-MM-dd HH:mm:ss";

  Connection                 conn;
  PreparedStatement          ps;
  ResultSet                  rs;
  SimpleDateFormat           sdf;
  Statement                  stmt;

  String                     idbServer;
  String                     iportNumber;
  String                     idbName;
  String                     idbUser;
  String                     idbPassword;
  String                     iUrl;

  public GreenplumConnection(String dbServer, String portNumber, String dbName, String dbUser, String dbPassword)
  {
    createConnection(dbServer, portNumber, dbName, dbUser, dbPassword);
    ps = null;
    rs = null;
    sdf = new SimpleDateFormat(DATE_FORMAT_NOW);
  }

  public void createConnection(String dbServer, String portNumber, String dbName, String dbUser, String dbPassword)
  {
    try
    {
      Class.forName("org.postgresql.Driver").newInstance();
      iUrl = "jdbc:postgresql://" + dbServer + ":" + portNumber + "/" + dbName + "?user=" + dbUser + "&password=" + dbPassword;
      conn = DriverManager.getConnection(iUrl);
      idbServer = dbServer;
      iportNumber = portNumber;
      idbUser = dbUser;
      idbPassword = dbPassword;
      idbName = dbName;
    }
    catch (SQLException se)
    {
      // handle any errors
      System.out.println("SQLException: " + se.getMessage());
      System.out.println("SQLState: " + se.getSQLState());
      System.out.println("VendorError: " + se.getErrorCode());
    }
    catch (Exception e)
    {
      // handle any errors
      System.out.println(e.getMessage());
      e.printStackTrace();
      System.exit(1);
    }
  }

  public GreenplumConnection(String dbLinkName, String fileName)
  {
    File f = new File(fileName);
    if (!f.exists())
    {
      System.out.println("File " + fileName + " doed not exist!!!");
      System.exit(1);
    }
    try
    {
      BufferedReader reader = new BufferedReader(new FileReader(f));
      String line = null;
      while ((line = reader.readLine()) != null)
      {
        String[] tokens = line.split(":");
        if (tokens[0].equalsIgnoreCase("greenplum") && tokens[1].equalsIgnoreCase(dbLinkName))
        {
          String[] connectionFields = tokens[2].split(",");
          createConnection(connectionFields[0], connectionFields[1], connectionFields[2], connectionFields[3], connectionFields[4]);
        }
      }
    }
    catch (IOException ioe)
    {
      System.out.println("File " + fileName + " cannot be read!!!");
      System.exit(1);
    }
    if (conn == null)
    {
      System.out.println("No connection could be established!!!");
      System.exit(1);
    }
  }

  public void closeConnectionObjects()
  {
    if (rs != null)
    {
      try
      {
        rs.close();
      }
      catch (SQLException sqlEx)
      {
      }
      rs = null;
    }

    if (ps != null)
    {
      try
      {
        ps.close();
      }
      catch (SQLException sqlEx)
      {
      }
      ps = null;
    }
  }

  public void closeConnection()
  {
    closeConnectionObjects();
    try
    {
      conn.close();
    }
    catch (SQLException sqlEx)
    {
    }
  }

  public void closeAll()
  {
    closeConnectionObjects();
    closeConnection();
  }

  public static GreenplumConnection getGreenplumConnection(String[] args)
  {
    GreenplumConnection tgc = null;
    String dbServer = "";
    String dbUser = "";
    String dbPassword = "";
    String dbName = "";
    String portNumber = "";
    String configFileName = "";
    String dbLinkName = "";

    int i = 0;
    while (i < args.length)
    {
      if (args[i].toLowerCase().equals("-config"))
      {
        try
        {
          configFileName = new String(args[i + 1]);
        }
        catch (Exception e)
        {
        }
        i++;
      }
      if (args[i].toLowerCase().equals("-dblink"))
      {
        try
        {
          dbLinkName = new String(args[i + 1]);
        }
        catch (Exception e)
        {
        }
        i++;
      }
      if (args[i].toLowerCase().equals("-s"))
      {
        try
        {
          dbServer = new String(args[i + 1]);
        }
        catch (Exception e)
        {
        }
        i++;
      }
      if (args[i].toLowerCase().equals("-db"))
      {
        try
        {
          dbName = new String(args[i + 1]);
        }
        catch (Exception e)
        {
        }
        i++;
      }
      if (args[i].toLowerCase().equals("-port"))
      {
        try
        {
          portNumber = new String(args[i + 1]);
        }
        catch (Exception e)
        {
        }
        i++;
      }
      if (args[i].toLowerCase().equals("-u"))
      {
        try
        {
          dbUser = new String(args[i + 1]);
        }
        catch (Exception e)
        {
        }
        i++;
      }
      if (args[i].toLowerCase().equals("-p"))
      {
        try
        {
          dbPassword = new String(args[i + 1]);
        }
        catch (Exception e)
        {
        }
        i++;
      }
      i++;
    }

    /*
    System.out.println("dbServer = " + dbServer);
    System.out.println("port = " + portNumber);
    System.out.println("dbName = " + dbName);
    System.out.println("dbUser = " + dbUser);
    System.out.println("dbPassword = " + dbPassword);
    */

    if ((configFileName == "") || (dbLinkName == ""))
    {
      if ((dbServer == "") || (dbUser == "") || (dbPassword == "") || (dbName == "") || (portNumber == ""))
      {
        System.out.println("Usage: java GreenplumConnection -s <Database Server> -db <Database Name> -port <Port> -u <Database User> -p <Database Password>");
        System.out.println("or:");
        System.out.println("Usage: java GreenplumConnection -config <Config File Name> -dblink <DB Link Name>");
        System.exit(1);
      }
      else
      {
        tgc = new GreenplumConnection(dbServer, portNumber, dbName, dbUser, dbPassword);
      }
    }
    else
    {
      tgc = new GreenplumConnection(dbLinkName, configFileName);
    }

    return tgc;
  }

  public static void main(String[] args)
  {
    GreenplumConnection tgc = getGreenplumConnection(args);
    tgc.closeAll();
  }

}
