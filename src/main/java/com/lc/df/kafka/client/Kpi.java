package com.lc.df.kafka.client;

import java.io.File;
import java.io.IOException;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Date;
import java.util.HashMap;
import java.util.Hashtable;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.Map;
import java.util.Set;
import java.util.stream.Stream;

import org.apache.commons.io.FileUtils;
import org.apache.log4j.Logger;

/**
 * An HTTP server that sends back the content of the received HTTP request in a
 * pretty plaintext form.
 */
@SuppressWarnings("unused")
public class Kpi {
	private static LinkedList<Report> report = new LinkedList<Report>();
	private static LinkedList<ReportHtml> reportHTML = new LinkedList<ReportHtml>();

	public static int total_sent = 0;
	public static Map<String, Integer> messageTypeCount = new HashMap<String, Integer>();
	public static int total_success = 0;
	public static int total_error = 0;
	public static int total_error_timeout = 0;
	public static int total_error_connect = 0;
	public static int total_error_unknown = 0;
	private static SimpleDateFormat sdf = new SimpleDateFormat("yyyyMMddHHmmss");
	public static int fileIndex = 1;
	public static int total_discarded = 0;
	private static String statType = KafkaClientConfig.stats_format;
	public static String outFileName = null;
	private static String outFileNameHTML = null;
	public static long avg_latency = 0;
	public static long startSending = -1;
	public static long stopSending = 0;
	public static long sumLatency = 0;

	public static int sent_last_round = 0;

	private static Integer lock_sent = new Integer(1);
	private static Integer lock_recieved = new Integer(1);
	private static Integer lock_error = new Integer(1);

	private String correlation = null;

	private boolean sent = false;
	private boolean received = false;
	private boolean error = false;

	private String httpStatus = null;
	private String errorType = null;

	private long timeSent;
	private long timeReceived;
	private long timetimeout;
	private long timeError;

	private long lat_connecting;
	private long lat_sending;
	private long lat_receiving;
	private long lat_total;
	protected String[] response_Payload = null;

	Logger log;
	ArrayList<String> failedProcesses = new ArrayList<String>();

	// Multiple files needs to considered as part of the new change
	static {
		new Thread(new KafkaClientStats()).start();
	}

	public Kpi(String correlation) {
		log = KafkaClient.getLogger();
		this.correlation = correlation;
	}
	public void setSent(String messageType) {
		sent = true;
		timeSent = System.currentTimeMillis();
		synchronized (lock_sent) {
			total_sent++;
			Integer count = messageTypeCount.get(messageType);
			if (count == null)
			{
			    messageTypeCount.put(messageType, 1);
			}
			else
			{
			    messageTypeCount.put(messageType, count + 1);
			}
		}
	}

	public void setReceived(String response, TransactionContext txContext) 
	{
		received = true;
		timeReceived = System.currentTimeMillis();
		String status = "200";
		lat_receiving = (timeSent > 0) ? (timeReceived - timeSent) : 0;
		lat_total = lat_receiving;
		boolean success = false;
		String responseMessage = "OK";
		
		long bytes = 0;
		if (response != null)
		{
			bytes = response.getBytes().length;
		}
		synchronized (lock_recieved) {
			sumLatency += lat_total;
			total_success++;
		}
		success = true;
		status = "200";
		addReport(new Report(lat_receiving, lat_total, success, status,
				responseMessage.equals("OK") ? "" : "failed", Thread.currentThread().getName(), bytes));
		logResult();
	}

	public boolean isSuccess() {
		return received;
	}

	public void setTimeout() {
		error = true;
		timeError = System.currentTimeMillis();
		errorType = "timeout";
		synchronized (lock_error) {
			total_error_timeout++;
		}

		logResult();
	}

	public static void setDiscarded() {
		total_discarded++;
	}

	public static long startSending() {
		if (startSending == -1) {
			startSending = System.currentTimeMillis();
		}
		return startSending;
	}

	public static long stopSending() {
		return stopSending = System.currentTimeMillis();

	}

	public static boolean hasStoppedSending() {
		return stopSending > 0;
	}

	public void setError(Object o) {
		error = true;
		timeError = System.currentTimeMillis();

		synchronized (lock_error) {
			total_error++;

			if (o instanceof java.net.ConnectException) {
				Exception ex = (Exception) o;
				errorType = ex.toString();
				total_error_connect++;
			} else {
				errorType = o.getClass().getName();
				total_error_unknown++;
			}
		}

		logResult();
	}

	private void logResult() {
		long latency = 0;

		if (received)
			latency = timeReceived - timeSent;

		if (KafkaClientConfig.file_stats)
		{
			log.info( 
					correlation + "," +
							timeReceived + "," + timeSent + "," + lat_receiving + "," + lat_total + "," + httpStatus
							+ "," + error + "," + errorType);
			
		}
	}

	public static String getStats() {
		return getStats(false);
	}
	public static String getStats(boolean includeMessageCounts) {
		long stopWatch = stopSending;

		// if stopWatch = 0 it means threads are keep sending requests.
		// we then then take the current time to measure current TPS
		if (stopWatch == 0)
			stopWatch = System.currentTimeMillis();
		long sending_time = stopWatch - startSending;
		long avg_latency = 0;
		if (total_success != 0)
			avg_latency = sumLatency / total_success;

		// calculation for average TPS rate
		double t1 = (total_sent * 1000);
		double t2 = sending_time;
		double t3 = t1 / t2;
		long tps_avg = Math.round(t3);

		int stint = total_sent - sent_last_round;
		sent_last_round = total_sent;
		t1 = (stint * 1000);
		t2 = KafkaClientConfig.stats_interval_ms;
		t3 = t1 / t2;
		long tps_last = Math.round(t3);

		KafkaClient.tps_avg = KafkaClient.tps_avg + tps_avg;
		KafkaClient.tps_last = KafkaClient.tps_last + tps_last;
		KafkaClient.sending_time = KafkaClient.sending_time + sending_time;
		KafkaClient.total_sent = KafkaClient.total_sent + total_sent;
		KafkaClient.sumLatency = KafkaClient.sumLatency + sumLatency;
		KafkaClient.total_success = KafkaClient.total_success + total_success;
		KafkaClient.total_error = KafkaClient.total_error + total_error;
		KafkaClient.total_error_connect = KafkaClient.total_error_connect + total_error_connect;
		KafkaClient.total_error_timeout = KafkaClient.total_error_timeout + total_error_timeout;
		KafkaClient.total_discarded = KafkaClient.total_discarded + total_discarded;
		KafkaClient.total_error_unknown = KafkaClient.total_error_unknown + total_error_unknown;
		KafkaClient.avg_latency = KafkaClient.avg_latency + avg_latency;

		String returnStr = "TPS(avg):" + tps_avg + ", TPS(last):" + tps_last + ", TIME: " + sending_time + ", sent:" + total_sent
			+ ", ok:" + total_success + ", error:" + total_error + "(c/t/d/o: " + total_error_connect + "/"
			+ total_error_timeout + "/" + total_discarded + "/" + total_error_unknown + "), avg_latency:"
			+ avg_latency;

		// If requested, add counts of each different message type. They are sorted in alphabetical
		// order for convenience.
		if (includeMessageCounts)
		{
			Stream<Map.Entry<String, Integer>> sorted = messageTypeCount.entrySet().stream()
				.sorted(Map.Entry.comparingByKey());
			Iterator<Map.Entry<String, Integer>> iter = sorted.iterator();
			returnStr += "\n  Message type counts:";
			while (iter.hasNext()) {
				Map.Entry<String, Integer> e = iter.next();
				returnStr += String.format("\n    %s=%d", e.getKey(), e.getValue());
			}
			Integer selectionUpdateCounts = messageTypeCount.get("selection-update");
			if (selectionUpdateCounts != null)
			{
				returnStr += String.format("\n  Percentage of messages for selection updates: %.1f%%", (float) selectionUpdateCounts / total_sent * 100);
			}
		}

		return returnStr;
	}

	public long getLatency() {
		return timeReceived - timeSent;
	}

	private static synchronized void addReport(Report jmsReport) {

		report.addLast(jmsReport);

	}

	private static synchronized LinkedList<Report> getReports() {
		LinkedList<Report> currentSet = new LinkedList<Report>();
		currentSet.addAll(report);
		report.clear();
		return currentSet;
	}


	private static synchronized LinkedList<ReportHtml> getHTMLReports() {
		LinkedList<ReportHtml> currentSetHTML = new LinkedList<ReportHtml>();
		currentSetHTML.addAll(reportHTML);
		reportHTML.clear();
		return currentSetHTML;
	}

	public static void writeReport() {
		// Logger.logInfoMessage("������ Report Size"+report.size());
		if (!KafkaClientConfig.file_stats) {
			return;
		}
		try {
			if (outFileName == null) {
				Date dt = new Date();
				String extension = ".csv";
				if ("JTL".equalsIgnoreCase(statType)) {
					extension = ".jtl";
				}
				outFileName = KafkaClientConfig.stats_file_name + "_" + sdf.format(dt) + "_" + fileIndex + extension;
				outFileNameHTML = KafkaClientConfig.stats_file_name + "_" + sdf.format(dt) + "_" + fileIndex + ".html";
				// Logger.logInfoMessage("FileName created");

			}
			File output = new File(outFileName);
			if ("JTL".equalsIgnoreCase(statType)) {
				if (!output.exists()) {
					// Logger.logInfoMessage("File doesn't exist, going to
					// create");
					FileUtils.write(output,
							"<?xml version=\"1.0\" encoding=\"UTF-8\"?>\n<testResults version=\"1.2\">\n", true);
				}

			}
			// 0 if (total_sent >= 1 || (isSingleRequest() && total_sent == 1))
			if (true) {

				completeReport();
				fileIndex++;
				Date dt = new Date();
				String extension = ".csv";
				if ("JTL".equalsIgnoreCase(statType)) {
					extension = ".jtl";
				}
				outFileName = KafkaClientConfig.stats_file_name + "_" + sdf.format(dt) + "_" + fileIndex
						+ extension;
			}

		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}

	}

	public static void completeReport() {
		if (!KafkaClientConfig.file_stats) {
			return;
		}
		try {
			if ("JTL".equalsIgnoreCase(statType)) {
				File file = new File(outFileName);
				File fileHTML = new File(outFileNameHTML);
				writeCurrentSet(file);

				com.lc.df.kafka.client.Logger.logInfoMessage("HTML Reports written");
				writeCurrentSetHTML(fileHTML);
				FileUtils.write(fileHTML, "</table></body></html>", true);
				FileUtils.write(file, "</testResults>", true);

			}
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}

	}

	private static void writeCurrentSet(File file) throws IOException {
		// Logger.logInfoMessage("WriteReports Called"+file.toString());
		LinkedList<Report> current = getReports();
		for (Iterator<Report> iterator = current.iterator(); iterator.hasNext();) {
			Report jmsReport = iterator.next();
			FileUtils.write(file, jmsReport.toString(), true);
			// FileUtils.write(fileHTML, "<tr>"+ad,true);
		}
		//
		Hashtable<String, TransactionContext> remaining = new Hashtable<String, TransactionContext>();//TransactionContext.getRemainingTransactionContext();
		// Logger.logInfoMessage("TransactionsContext"+remaining.toString());
		Set<String> keys = remaining.keySet();
		// Logger.logInfoMessage("TransactionsContext"+keys.size());
		for (String key : keys) {
			// Logger.logInfoMessage("Value of "+key+" is: "+remaining.get(key));
			TransactionContext tscCtxt = remaining.get(key);
			if (tscCtxt != null) {
				// tscCtxt.getRespFile();
				// Logger.logInfoMessage("Response Not received for ");
				Kpi kpiObj = tscCtxt.getKpi();
				FileUtils
						.write(file,
								new Report(kpiObj.lat_receiving, kpiObj.lat_total, false, "500",
										"Response Not Received ", Thread.currentThread().getName(), 0).toString(),
								true);
			}
		}
		//

	}

	private static void writeCurrentSetHTML(File fileHTML) throws IOException {
		com.lc.df.kafka.client.Logger.logInfoMessage("WriteReports HTML Called" + fileHTML.toString());
		int counter = 1;
		String label = null;
		LinkedList<ReportHtml> current = getHTMLReports();
		for (Iterator<ReportHtml> iterator = current.iterator(); iterator.hasNext();) {
			ReportHtml jmsReportHTML = iterator.next();
			String status = jmsReportHTML.isSuccess() ? "Success" : "Failure";
			label = jmsReportHTML.getLabel();
			if (status.equalsIgnoreCase("Success")) {
				FileUtils.write(fileHTML,
						"<tr>" + "<td bgcolor=\"#00FF00\">" + counter + "</td>" + "<td bgcolor=\"#00FF00\">" + label
								+ "</td>" + "<td bgcolor=\"#00FF00\">" + jmsReportHTML.getTestCaseName() + "</td>"
								+ "<td bgcolor=\"#00FF00\">" + status + "</td>" + "</tr>",
						true);
			} else {
				FileUtils.write(fileHTML,
						"<tr>" + "<td bgcolor=\"#F76E6E\">" + counter + "</td>" + "<td bgcolor=\"#F76E6E\">" + label
								+ "</td>" + "<td bgcolor=\"#F76E6E\">" + jmsReportHTML.getTestCaseName() + "</td>"
								+ "<td bgcolor=\"#F76E6E\">"
								+ jmsReportHTML.getResponseMessage().replace(
										"junit.framework.AssertionFailedError: The difference is, org.custommonkey.xmlunit.Diff",
										"")
								+ "</td>" + "</tr>",
						true);
			}
			counter++;
		}
		Hashtable<String, TransactionContext> remaining = new Hashtable<String, TransactionContext>();//TransactionContext.getRemainingTransactionContext();
		// Logger.logInfoMessage("TransactionsContext"+remaining.toString());
		Set<String> keys = remaining.keySet();
		com.lc.df.kafka.client.Logger.logInfoMessage("TransactionsContext" + keys.size());
		for (String key : keys) {
			TransactionContext tscCtxt = remaining.get(key);
		}

	}

	private static boolean isSingleRequest() {
		return KafkaClientConfig.client_tps == 1 && KafkaClientConfig.client_maxtime == 1;
	}
}
