package jdbc;

import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.Calendar;

import org.postgresql.copy.CopyManager;
import org.postgresql.core.BaseConnection;
import java.io.File;
import java.io.BufferedWriter;
import java.io.FileReader;
import java.io.FileWriter;
import java.io.IOException;

import util.ParseParameters;
import util.gpfdistController;

public class DeleteRecords
{

	GreenplumConnection gpcon;
	GreenplumConnection gpcon2;
	String whereClause;
	String fileName;
	String sourceTable;
	String[] myArgs;
	String fileBaseName;
	
	public DeleteRecords(String[] args) throws SQLException
	{
		myArgs = args;
		sourceTable = "rupen.fact01";
		gpcon = GreenplumConnection.getGreenplumConnection(args);
		gpcon2 = GreenplumConnection.getGreenplumConnection(args);
		gpcon.conn.setAutoCommit(false);
		whereClause = ParseParameters.findAttribute("filter", args);
		gpcon.stmt = gpcon.conn.createStatement();
		gpcon2.stmt = gpcon2.conn.createStatement();
		fileName = (new File(".").getAbsolutePath()) + "/" + "temp_" + sourceTable + ".lst";
		fileBaseName = new File(fileName).getName();
	}

	public String stripSchemaName (String tableNameWithSchema)
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
	
	public void createTempTable (String tableName) throws SQLException
	{
		gpcon2.stmt.execute("create table " + tableName + " (like " + sourceTable + ")");
	}
	
	public void truncateTempTable (String tableName) throws SQLException
	{
		gpcon2.stmt.execute("truncate table " + tableName);
	}
	
	public int deleteFromTempTable (String tableName) throws SQLException
	{
		int deleted = 0;
		gpcon2.ps = gpcon2.conn.prepareStatement("select txndt from " + tableName + " group by txndt order by 1");
		ArrayList<String> txndtlist = new ArrayList<String>(10);
		gpcon2.rs = gpcon2.ps.executeQuery();
		while (gpcon2.rs.next())
		{
			txndtlist.add(gpcon2.rs.getString(1));
		}
		gpcon2.rs.close();
		gpcon2.ps.close();
		String[] txndts = txndtlist.toArray(new String[txndtlist.size()]);

		String sqlUpdate =
		"delete from @tableName                                                          \n" +
		"using  @tempTableName t                                                         \n" +
		"where  t.id1 = @tableName.id1                                                   \n" +
		"and    t.id2 = @tableName.id2                                                   \n" +
		"and    t.id3 = @tableName.id3                                                   \n" +
		"and    @tableName.txndt = '@txndt'::timestamp                                     ";
		
		for (int i=0; i<txndts.length; i++)
		{
			String sql = sqlUpdate.replaceAll("@tableName", sourceTable);
			sql = sql.replaceAll("@tempTableName", tableName);
			sql = sql.replaceAll("@txndt", txndts[i]);
			//System.out.println(sql);
			int myDeletes = gpcon2.stmt.executeUpdate(sql);
			deleted += myDeletes;
			System.out.println ("Deleted " + myDeletes + " records for txndt = " + txndts[i]);
		}
		
		return deleted;
	}

	public int deleteRecords(boolean usegpfdist) throws SQLException, IOException, InterruptedException
	{
		int cntInserted = 0;
		int cntNotInserted = 0;
		int cntRead = 0;
		int cntDeleted = 0;
		String tempTableName = "temp_" + stripSchemaName(sourceTable);
		BufferedWriter writer = new BufferedWriter(new FileWriter (new File(fileName)));
		try
		{
			createTempTable (tempTableName);
		}
		catch (Exception e)
		{
			gpcon2.stmt.close();
			gpcon2.stmt = gpcon2.conn.createStatement();
			truncateTempTable (tempTableName);
		}

		String sql = "select * from " + sourceTable;
		if (whereClause != null && ! whereClause.trim().equals(""))
		{
			sql = sql + " " + whereClause;
		}
		gpcon.stmt.setFetchSize(10000);
		gpcon.rs = gpcon.stmt.executeQuery(sql);
		ResultSetMetaData md = gpcon.rs.getMetaData();
		int colCount = md.getColumnCount();
		StringBuilder line = new StringBuilder(200);
		CopyManager copyManager = null;
		if (! usegpfdist)
		{
			copyManager = new CopyManager((BaseConnection) gpcon2.conn);
		}
		while (gpcon.rs.next())
		{
			cntRead++;
			cntNotInserted++;
			line = new StringBuilder(200);
			for (int i=1; i<=colCount; i++)
			{
                if (i>1)
                {
                    line.append("\t");
                }
                String val = gpcon.rs.getString(i);
                if (md.getColumnName(i).equalsIgnoreCase("utime"))
                {
                	val = Calendar.getInstance().getTime().toString();
                }
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
				if (! usegpfdist)
				{
					writer.close();
					copyManager.copyIn("copy " + tempTableName + " from stdin null as ''", new FileReader(fileName));
					writer = new BufferedWriter(new FileWriter (new File(fileName)));
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
		if (! usegpfdist && cntNotInserted > 0)
		{
			copyManager.copyIn("copy " + tempTableName + " from stdin null as ''", new FileReader(fileName));
			cntInserted += cntNotInserted;
		}
		if (usegpfdist)
		{
			gpfdistController gpfdist = new gpfdistController (".", ".");
			gpfdist.startServer();
			String extTabName = "ext_" + stripSchemaName (sourceTable);
			gpcon2.stmt.executeUpdate("drop external table if exists " + extTabName);
			
			String exttabSql =
			"create external table @extTabName                                       \n" +
			"(like @tableName)                                                       \n" +
			"location ('gpfdist://mdw:@portNumber/@fileName')                        \n" +
			"format 'text' (null '')                                                 \n";
			
			exttabSql = exttabSql.replaceAll("@extTabName", extTabName);
			exttabSql = exttabSql.replaceAll("@tableName", sourceTable);
			exttabSql = exttabSql.replaceAll("@portNumber", gpfdist.getPortNumber());
			exttabSql = exttabSql.replaceAll("@fileName", fileBaseName);
			gpcon2.stmt.executeUpdate(exttabSql);
			
			String exttabInsertSql = "insert into " + tempTableName + " select * from " + extTabName;
			cntInserted = gpcon2.stmt.executeUpdate(exttabInsertSql);
			
			gpfdist.stopServer();
		}
		cntDeleted = deleteFromTempTable(tempTableName);
		truncateTempTable (tempTableName);
		gpcon2.closeAll();
		new File(fileName).delete();
		return cntDeleted;
	}

	public static void main (String[] args) throws Exception
	{
		DeleteRecords dr = new DeleteRecords(args);
		boolean usegpfdist = ParseParameters.existsAttribute("-gpfdist", args);
		int cnt = dr.deleteRecords(usegpfdist);
		System.out.println ("Records Deleted = " + cnt);
	}
	
}
