package util;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;

public class gpfdistController
{
	String portNumber;
	String pid;
	String dir;
	String logDir;
	String logFileName;
	Process process;
	String command;
	
	public gpfdistController (String dir, String logDir)
	{
		this.dir = dir;
		this.logDir = logDir;
		this.logFileName = logDir + "/" + "gpfdist.log";
		this.command = "nohup gpfdist -p 8000 -P 9000 -d " + dir + " -l " + logFileName;
	}
	
	public void startServer() throws IOException, InterruptedException
	{
		System.out.println(command);
		String[] cmdArray = command.split(" ");
		this.process = Runtime.getRuntime().exec(cmdArray);
		Thread.sleep(2000);
		BufferedReader reader = new BufferedReader(new InputStreamReader(process.getInputStream()));
		String line;
		while ((line = reader.readLine()) != null)
		{
			// System.out.println("Line = " + line);
			if (line.startsWith("Serving HTTP"))
			{
				String[] words = line.split(" ");
				for (int i=0; i<words.length; i++)
				{
					if (words[i].equals("port"))
					{
						portNumber = words[i+1].replaceAll(",*$", "");
						break;
					}
				}
			}
		}
		reader.close();
		reader = new BufferedReader(new InputStreamReader(process.getErrorStream()));
		while ((line = reader.readLine()) != null)
		{
			System.out.println("Error Line = " + line);
		}
		reader.close();
		System.out.println("gpfdist port = " + portNumber);
		//getPID();
	}
	
	public String getPortNumber ()
	{
		return portNumber;
	}
	
	public void getPID() throws IOException, InterruptedException
	{
		//String cmd = "netstat --all --programs --numeric-ports 2>/dev/null | grep gpfdist | grep 'LISTEN' | awk '{print $4,$7}'";
		String cmd = "./gpfdist_ports.sh";
		Process p = Runtime.getRuntime().exec(cmd);
		p.waitFor();
		BufferedReader reader = new BufferedReader(new InputStreamReader(p.getInputStream()));
		String line;
		while ((line = reader.readLine()) != null)
		{
			String[] parms = line.split("\\s");
			if (parms[0].matches("^\\*:" + portNumber))
			{
				this.pid = parms[1].split("/")[0];
			}
		}
		reader.close();
		System.out.println ("Process ID = " + this.pid);
	}
	
	public void stopServer() throws IOException, InterruptedException
	{
		/*
		if (pid != null && ! pid.trim().equals(""))
		{
			String cmd = "kill " + pid;
			Process p = Runtime.getRuntime().exec(cmd);
			p.waitFor();
		}
		*/
		process.destroy();
	}
	
	public static void main (String[] args) throws IOException, InterruptedException
	{
		String dir = ParseParameters.findAttribute("dir", args);
		String logDir = ParseParameters.findAttribute("logdir", args);
		System.out.println("dir = " + dir);
		System.out.println("logDir = " + logDir);
		if (dir == null || dir.trim().equals(""))
		{
			printUsage();
			System.exit(1);
		}
		if (logDir == null || logDir.trim().equals(""))
		{
			printUsage();
			System.exit(1);
		}
		gpfdistController gpfdist = new gpfdistController (dir, logDir);
		gpfdist.startServer();
		System.out.println("gpfdist started..");
		if (ParseParameters.existsAttribute("stop", args))
		{
			System.out.println("waiting 20 seconds to stop gpfdist..");
			Thread.sleep(20000);
			System.out.println("stopping gpfdist..");
			gpfdist.stopServer();
		}
	}
	
	public static void printUsage()
	{
		System.out.println ("Usage: java util.gpfdistController -dir <Directory> -logDir <Log Directory>");
	}

}
