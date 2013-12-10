package jdbc;

import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import org.postgresql.copy.CopyManager;
import org.postgresql.core.BaseConnection;
import java.io.File;
import java.io.BufferedWriter;
import java.io.FileReader;
import java.io.FileWriter;
import java.io.IOException;

import util.ParseParameters;
import util.gpfdistController;

public class InsertRecords
{
  GreenplumConnection gpcon;
  GreenplumConnection gpcon2;
  String              sourceTable;
  String              destTable;
  Boolean             createDestTable;
  Boolean             destTableTemp;
  String              whereClause;
  String              fileName;
  String              fileBaseName;

  public InsertRecords(String[] args) throws SQLException
  {
    gpcon = GreenplumConnection.getGreenplumConnection(args);
    gpcon2 = GreenplumConnection.getGreenplumConnection(args);
    gpcon.conn.setAutoCommit(false);
    sourceTable = ParseParameters.findAttribute("source", args);
    destTable = ParseParameters.findAttribute("dest", args);
    whereClause = ParseParameters.findAttribute("filter", args);
    try
    {
      createDestTable = ParseParameters.findAttribute("cdest", args).toLowerCase().equals("true");
    }
    catch (Exception e)
    {
      createDestTable = false;
    }

    try
    {
      destTableTemp = ParseParameters.findAttribute("dtemp", args).toLowerCase().equals("true");
    }
    catch (Exception e)
    {
      destTableTemp = false;
    }

    gpcon2.stmt = gpcon2.conn.createStatement();

    if (createDestTable)
    {
      gpcon2.stmt.execute("create " + (destTableTemp ? "temporary" : "") + " table " +
                destTable + " (like " + sourceTable + ")"
          );
      // gpcon2.ps.execute();
      gpcon2.closeAll();
      gpcon2 = GreenplumConnection.getGreenplumConnection(args);
    }
    gpcon.stmt = gpcon.conn.createStatement();
    fileName = (new File(".").getAbsolutePath()) + "/" + "temp_" + sourceTable + ".lst";
    fileBaseName = new File(fileName).getName();
  }

  public String stripSchemaName(String tableNameWithSchema)
  {
    String[] names = tableNameWithSchema.split("\\.");
    if (names.length == 1)
    {
      return names[0];
    }
    else
    {
      return names[1];
    }
  }

  public void createTempTable(String tableName) throws SQLException
  {
    gpcon.stmt.execute("create temporary table " + tableName + " (like " + sourceTable + ")");
  }

  public void truncateTempTempTable(String tableName) throws SQLException
  {
    gpcon.stmt.execute("truncate table " + tableName);
  }

  public int insertRecords(boolean usegpfdist) throws SQLException, IOException, InterruptedException
  {
    int cntInserted = 0;
    int cntNotInserted = 0;
    int cntRead = 0;
    BufferedWriter writer = new BufferedWriter(new FileWriter(new File(fileName)));
    String sql = "select * from " + sourceTable;
    if (whereClause != null && !whereClause.trim().equals(""))
    {
      sql = sql + " " + whereClause;
    }
    gpcon.stmt.setFetchSize(10000);
    gpcon.rs = gpcon.stmt.executeQuery(sql);
    ResultSetMetaData md = gpcon.rs.getMetaData();
    int colCount = md.getColumnCount();
    StringBuilder line = new StringBuilder(200);
    CopyManager copyManager = null;
    if (!usegpfdist)
    {
      copyManager = new CopyManager((BaseConnection) gpcon2.conn);
    }
    while (gpcon.rs.next())
    {
      cntRead++;
      cntNotInserted++;
      line = new StringBuilder(200);
      for (int i = 1; i <= colCount; i++)
      {
        if (i > 1)
        {
          line.append("\t");
        }
        String val = gpcon.rs.getString(i);
        if (val == null)
        {
          val = "";
        }
        if (val != "" && (md.getColumnType(i) == 12 || md.getColumnType(i) == 1))
        {
          val = val.replaceAll("\\\\", "\\\\\\\\");
          val = val.replaceAll("\t", "\\\\t");
          val = val.replaceAll("\r\n", "\\\\n");
          val = val.replaceAll("\r", "\\\\n");
          val = val.replaceAll("\n", "\\\\n");
        }
        line.append(val);
      }
      line.append("\n");
      writer.write(line.toString());

      if (cntRead % 100000 == 0)
      {
        System.out.println("Records read = " + cntRead);
        if (!usegpfdist)
        {
          writer.close();
          copyManager.copyIn("copy " + destTable + " from stdin null as ''", new FileReader(fileName));
          writer = new BufferedWriter(new FileWriter(new File(fileName)));
          cntInserted += cntNotInserted;
          cntNotInserted = 0;
        }
      }
    }
    gpcon.rs.close();
    // gpcon.ps.close();
    gpcon.closeAll();
    writer.close();
    System.out.println("Records read = " + cntRead);
    if (!usegpfdist && cntNotInserted > 0)
    {
      copyManager.copyIn("copy " + destTable + " from stdin null as ''", new FileReader(fileName));
      cntInserted += cntNotInserted;
    }
    if (usegpfdist)
    {
      gpfdistController gpfdist = new gpfdistController(".", ".");
      gpfdist.startServer();
      String extTabName = "ext_" + stripSchemaName(sourceTable);
      gpcon2.stmt.executeUpdate("drop external table if exists " + extTabName);

      // @formatter:off
      String exttabSql =
      "create external table @extTabName                                       \n" +
      "(like @tableName)                                                       \n" +
      "location ('gpfdist://mdw:@portNumber/@fileName')                        \n" +
      "format 'text' (null '')                                                 \n";
      // @formatter:on

      exttabSql = exttabSql.replaceAll("@extTabName", extTabName);
      exttabSql = exttabSql.replaceAll("@tableName", sourceTable);
      exttabSql = exttabSql.replaceAll("@portNumber", gpfdist.getPortNumber());
      exttabSql = exttabSql.replaceAll("@fileName", fileBaseName);
      gpcon2.stmt.executeUpdate(exttabSql);

      String exttabInsertSql = "insert into " + destTable + " select * from " + extTabName;
      cntInserted = gpcon2.stmt.executeUpdate(exttabInsertSql);

      gpfdist.stopServer();
    }
    gpcon2.closeAll();
    new File(fileName).delete();
    return cntInserted;
  }

  public static void main(String[] args) throws Exception
  {
    InsertRecords ir = new InsertRecords(args);
    boolean usegpfdist = ParseParameters.existsAttribute("-gpfdist", args);
    int cnt = ir.insertRecords(usegpfdist);
    System.out.println("Records Inserted = " + cnt);
  }

}
