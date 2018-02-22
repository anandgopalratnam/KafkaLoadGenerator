package com.lc.df.kafka.client;



public class ReportHtml
{
	/*
	 * by Bytes
		de Data encoding
	dt Data type
	ec Error count (0 or 1, unless multiple samples are aggregated)
	hn Hostname where the sample was generated
	it Idle Time = time not spent sampling (milliseconds) (generally 0)
	lb Label
	lt Latency = time to initial response (milliseconds) - not all samplers support this
	ct Connect Time = time to establish the connection (milliseconds) - not all samplers support this
	na Number of active threads for all thread groups
	ng Number of active threads in this group
	rc Response Code (e.g. 200)
	rm Response Message (e.g. OK)
	s Success flag (true/false)
	sc Sample count (1, unless multiple samples are aggregated)
	t Elapsed time (milliseconds)
	tn Thread Name
	ts timeStamp (milliseconds since midnight Jan 1, 1970 UTC)*/
	
	long latency = -1;
	public long getLatency() {
		return latency;
	}

	public void setLatency(long latency) {
		this.latency = latency;
	}

	public long getInitLatency() {
		return initLatency;
	}

	public void setInitLatency(long initLatency) {
		this.initLatency = initLatency;
	}

	public boolean isSuccess() {
		return success;
	}

	public void setSuccess(boolean success) {
		this.success = success;
	}

	public String getResponseCode() {
		return responseCode;
	}

	public void setResponseCode(String responseCode) {
		this.responseCode = responseCode;
	}

	public String getResponseMessage() {
		return responseMessage;
	}

	public void setResponseMessage(String responseMessage) {
		this.responseMessage = responseMessage;
	}

	public String getThreadName() {
		return threadName;
	}

	public void setThreadName(String threadName) {
		this.threadName = threadName;
	}

	public long getBytes() {
		return bytes;
	}

	public void setBytes(long bytes) {
		this.bytes = bytes;
	}

	public long getTs() {
		return ts;
	}

	public void setTs(long ts) {
		this.ts = ts;
	}

	public String getLabel() {
		return label;
	}

	public void setLabel(String label) {
		this.label = label;
	}

	long initLatency = -1;
	boolean success = false;
	String responseCode = "200";
	String responseMessage = "";
	String threadName= null;
	long bytes = 0L;
	long ts = System.currentTimeMillis(); 
	String label = KafkaClientConfig.stats_label;
	String testCaseName="unknown";
	public ReportHtml(long latency,long initLatency, boolean success,String responseCode,String responseMessage,String threadName,long bytes, String testCaseName)
	{
		this.latency = latency;
		this.initLatency = initLatency;
		this.success = success;
		this.responseCode = responseCode;
		this.responseMessage = responseMessage;
		this.threadName = threadName;
		this.bytes = bytes;
		this.testCaseName =testCaseName;
	}
	
	public String getTestCaseName() {
		return testCaseName;
	}

	public void setTestCaseName(String testCaseName) {
		this.testCaseName = testCaseName;
	}

	public String toString()
	{
		String sample = "<httpSample t=\"" + latency + "\" " +
		"lt=\"" + initLatency + "\" " +
		"ts=\"" + ts + "\" " +
		"s=\"" + success + "\" " +
		"lb=\""+label+"\" " +
		"rc=\""	+ responseCode + "\" " +
		"rm=\""	+ responseMessage + "\" " +
		"tn=\""	+ threadName+ "\" " +
		"dt=\"text\" " +
		"by=\""+bytes+"\" " +
		"ng=\"1\" " +
		"na=\"3\"/>\n";
		return sample;
	}
}
