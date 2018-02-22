package com.lc.df.kafka.client;


public class Report
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
	long initLatency = -1;
	boolean success = false;
	String responseCode = "200";
	String responseMessage = "";
	String threadName= null;
	long bytes = 0L;
	long ts = System.currentTimeMillis(); 
	String label = KafkaClientConfig.stats_label;
	
	public Report(long latency,long initLatency, boolean success,String responseCode,String responseMessage,String threadName,long bytes)
	{
		this.latency = latency;
		this.initLatency = initLatency;
		this.success = success;
		this.responseCode = responseCode;
		this.responseMessage = responseMessage;
		this.threadName = threadName;
		this.bytes = bytes;
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
