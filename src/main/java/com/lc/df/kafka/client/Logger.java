package com.lc.df.kafka.client;

import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.util.Properties;

import org.apache.log4j.Priority;
import org.apache.log4j.PropertyConfigurator;
import org.apache.log4j.helpers.LogLog;

public class Logger
{
	private static boolean loggingenabled = false;
	private static final org.apache.log4j.Logger logger = org.apache.log4j.Logger.getLogger(Logger.class.getName());
	private static Properties loggerprops = new Properties(); 
	protected static String configRoot = System.getProperty("config");

	static
	{
		try
		{
			if(configRoot == null)
			{
				configRoot = "./config";
			}
			loggerprops.load(new FileInputStream(configRoot + File.separator + "Logger.properties"));
			configure();
		}
		catch (FileNotFoundException e)
		{
			System.out.println("######### Error File Not Found for kafkaloadloggerconfig.. Adding default config to ./logs");
			LogLog.error("File Not Found for kafkaloadloggerconfig.. Adding default config to ./log ", e);
		}
		catch (IOException e)
		{
			System.out.println("######### Exception in getting Logger.properties Adding default config to ./logs");
			LogLog.error("IO Exception while reading file kafkaloadloggerconfig ", e);
		}
		catch(Exception ex)
		{
			ex.printStackTrace();
		}
		finally
		{
			System.out.println("###### Using Default Properties ##########");
			if (loggerprops.isEmpty())
			{
				loggerprops.put("fileout", "true");
				loggerprops.put("consoleout", "true");
				loggerprops.put("LogLevel", "INFO");
				loggerprops.put("logpattern", "[%t][%d{DATE}] %-5p %x - %m%n");
				loggerprops.put("consolelogpattern", "%m%n");
				loggerprops.put("filename", "./logs/kafkaloadgenerator.log");
				loggerprops.put("maxFileSize", "20MB");
				loggerprops.put("maxBackupSize", "100");
			}
		}
	}

	private static void configure()
	{
		Properties props = new Properties();
		boolean filelogging = new Boolean(loggerprops.getProperty("fileout")).booleanValue();
		boolean consolelogging = new Boolean(loggerprops.getProperty("consoleout")).booleanValue();
		String config = loggerprops.getProperty("LogLevel");
		String logPattern = loggerprops.getProperty("logpattern");
		String consoleLogPattern = loggerprops.getProperty("consolelogpattern");
		if (filelogging)
		{
			config = config + ",rollfile";
			props.put("log4j.appender.rollfile", "org.apache.log4j.RollingFileAppender");
			props.put("log4j.appender.rollfile.File", loggerprops.getProperty("filename", "./logs/kafkaloadgenerator.log"));
			props.put("log4j.appender.rollfile.MaxFileSize", loggerprops.getProperty("maxFileSize", "20MB"));
			props.put("log4j.appender.rollfile.MaxBackupIndex", loggerprops.getProperty("maxBackupSize", "100"));
			props.put("log4j.appender.rollfile.layout", "org.apache.log4j.PatternLayout");
			props.put("log4j.appender.rollfile.layout.ConversionPattern", logPattern);
		}
		if (consolelogging)
		{
			config = config + ",Console";
			props.put("log4j.appender.Console", "org.apache.log4j.ConsoleAppender");
			props.put("log4j.appender.Console.layout", "org.apache.log4j.PatternLayout");
			props.put("log4j.appender.Console.layout.ConversionPattern", consoleLogPattern);
		}
		props.put("log4j.category." + Logger.class.getName(), config);
		loggingenabled = filelogging | consolelogging;
		logger.removeAllAppenders();
		new PropertyConfigurator().doConfigure(props, logger.getLoggerRepository());
	}

	public static void logInfoMessage(Object message)
	{
		log(Priority.INFO, message);
	}

	public static void logDebugMessage(Object message)
	{
		log(Priority.DEBUG, message);
	}

	public static void logErrorMessage(Object message)
	{
		log(Priority.ERROR, message);
	}

	public static void logWarningMessage(Object message)
	{
		log(Priority.WARN, message);
	}

	public static void logErrorMessage(Object message, Throwable e)
	{
		log(Priority.ERROR, message, e);
	}

	public static void logWarningMessage(Object message, Throwable e)
	{
		log(Priority.WARN, message, e);
	}

	private static void log(Priority priority, Object message)
	{
		if ((loggingenabled) && (logger.getLevel().toInt() <= priority.toInt()))
		{
			Throwable tracer = new Throwable();
			tracer.fillInStackTrace();
			StringBuffer buffer = new StringBuffer();//getCallingClass(tracer));
			buffer.append(message);
			logger.log(priority, buffer.toString());
		}
	}

	private static void log(Priority priority, Object message, Throwable e)
	{
		if ((loggingenabled) && (logger.getLevel().toInt() <= priority.toInt()))
		{
			StringBuffer buffer = new StringBuffer();//getCallingClass(e));
			buffer.append(message);
			logger.log(priority, buffer.toString(), e);
		}
	}

	private static String getCallingClass(Throwable e)
	{
		StackTraceElement[] stack = e.getStackTrace();
		if ((stack == null) || (stack.length == 0))
		{
			return "[Unidentified Calling Class]";
		}
		StringBuffer buffer = new StringBuffer("[");
		buffer.append(stack[(stack.length - 1)].toString());
		buffer.append("]");
		return buffer.toString();
	}
}