package com.lc.df.kafka.client;

import java.util.LinkedList;
import java.util.Properties;
import java.io.*;

/**
 * An HTTP server that sends back the content of the received HTTP request in a
 * pretty plaintext form.
 */
public class KafkaClientConfig {

	private static String PROP_FILE;

	public static String topic_request;
	public static int threads_produce;
	public static int threads_consume;
	public static long listen_timeout;
	public static int client_tps;
	public static int client_maxtime;
	public static long max_records_per_file = 0;

	// statistics
	public static String stats_file_name;
	public static boolean file_stats = false;
	public static long stats_interval_ms;
	public static String stats_format = null;
	public static String stats_label = null;

	public static int payload_size;
	public static String payload_file;
	public static String payload_dir;
	public static String payload_fileprefix;
	public static String id_fileprefix;
	
	public static int inplay_seconds;
	public static int result_seconds;

	public static String kafka_bootstrap_servers;
	public static String kafka_topic;
	public static String kafka_compressiontype;	
	
	public static String kafka_listener_groupName ;
	public static int kafka_listener_maxpollrecords;
	
	public static long eventid_min ;
	public static long eventid_max ;
	public static int marketid_prefix_min ;
	public static int marketid_prefix_max ;
	public static int selectionid_prefix_min ;
	public static int selectionid_prefix_max ;
	
	public static int selection_priceupdate_multiplier ;
	
	public static LinkedList<String> topLevelList = new LinkedList<String>() ;
	public KafkaClientConfig(String propFile) {
		PROP_FILE = propFile;
	}

	public void init() {
		Logger.logInfoMessage("Using properties file: " + PROP_FILE);

		try {
			// InputStream is =
			// tibjmsClientConfig.class.getResourceAsStream(PROP_FILE);
			InputStream is = new FileInputStream(PROP_FILE);
			Properties prop = new Properties();
			prop.load(is);

			client_tps = Integer.valueOf(prop.getProperty("client.TPS"))
					.intValue();
			client_maxtime = Integer
					.valueOf(prop.getProperty("client.maxtime")).intValue();

			payload_size = Integer.valueOf(
					prop.getProperty("client.producer.payload.size.bytes"))
					.intValue();
			payload_file = prop.getProperty("client.producer.payload.file");
			payload_dir = prop.getProperty("client.producer.payload.dir");
			payload_fileprefix = prop.getProperty("client.producer.payload.fileprefix");
			id_fileprefix = prop.getProperty("client.producer.payload.idfileprefix");
			
			threads_produce = Integer.valueOf(
					prop.getProperty("client.producer.threads")).intValue();
			threads_consume = Integer.valueOf(prop.getProperty("client.listener.threads")).intValue();

			listen_timeout = Long.valueOf(
					prop.getProperty("client.listener.timeout.millis"))
					.longValue();
			String temp = prop.getProperty("stats.file.enabled");
			if (temp.equals("true"))
				file_stats = true;

			stats_interval_ms = Long.valueOf(
					prop.getProperty("stats.interval.millis")).longValue();
			stats_file_name = prop.getProperty("stats.file.name");
			System.setProperty("logfilename", stats_file_name);
			stats_format = prop.getProperty("stats.format");
			stats_label = prop.getProperty("stats.label");
			String maxRec = prop.getProperty("stats.maxrecords.perfile");


			if (maxRec == null || maxRec.length() == 0) {
				maxRec = "100000";
			}
			max_records_per_file = Long.valueOf(maxRec).longValue();

        		// Use bootstrap server info from the environment var if available
		        kafka_bootstrap_servers = System.getenv("KAFKA_BOOTSTRAP_SERVERS");
		        if (kafka_bootstrap_servers != null) {
		           System.out.printf("Overriding bootstrap.servers: %s\n", kafka_bootstrap_servers );
		        }
			else {
			   kafka_bootstrap_servers = prop.getProperty("kafka.bootstrap.servers");
		        }

			kafka_topic = prop.getProperty("kafka.topic");
			kafka_compressiontype = prop.getProperty("kafka.compressiontype");	
			kafka_listener_groupName = prop.getProperty("kafka.listener.groupName");
			kafka_listener_maxpollrecords = Integer.valueOf(prop.getProperty("kafka.listener.maxpollrecords")).intValue();
			
			String [] eventRangeSplit = prop.getProperty("client.producer.payload.eventid.range").split("-");
			eventid_min = Long.parseLong(eventRangeSplit[0]);
			eventid_max = Long.parseLong(eventRangeSplit[1]);
			String [] marketRangeSplit = prop.getProperty("client.producer.payload.marketid.prefix.range").split("-");
			marketid_prefix_min = Integer.parseInt(marketRangeSplit[0]);
			marketid_prefix_max = Integer.parseInt(marketRangeSplit[1]);
			String [] selectionRangeSplit = prop.getProperty("client.producer.payload.selectionid.prefix.range").split("-");
			selectionid_prefix_min = Integer.parseInt(selectionRangeSplit[0]);
			selectionid_prefix_max = Integer.parseInt(selectionRangeSplit[1]);
			
			selection_priceupdate_multiplier = Integer.parseInt(prop.getProperty("client.producer.payload.selection.priceupdate.multiplier"));
			
			inplay_seconds = Integer.parseInt(prop.getProperty("client.producer.inplaytime"));
			result_seconds = Integer.parseInt(prop.getProperty("client.producer.resulttime"));
			int totalTopLevels = Integer.parseInt(prop.getProperty("client.producer.payload.toplevels.count"));
			String topLevelDefault = prop.getProperty("client.producer.payload.toplevels.default");
			long totalEvents = eventid_max - eventid_min + 1;
			for (int i = 0; i < totalTopLevels; i++) {
				if (topLevelList.size() >= totalEvents){
					break;
				}
				String topLevelString = prop.getProperty("client.producer.payload.toplevels."+i);
				int indexOfColon = topLevelString.indexOf(':');
				if (indexOfColon > 0){
					String topLevel = topLevelString.substring(0,indexOfColon);
					int percentage = Integer.parseInt(topLevelString.substring(indexOfColon+ 1));
					long countToAdd = (percentage * totalEvents)/100;
					if (countToAdd == 0 ){
						countToAdd = 1;
					}
					for (int j = 0; j < countToAdd; j++) {
						addParentString(topLevel);
					}
				}
				else{
					addParentString(topLevelDefault);
				}
			}
			if (topLevelList.size() < totalEvents){
				for (int i = 0; i < (totalEvents - topLevelList.size()); i++) {
					addParentString(topLevelDefault);
				}
			}
			
			is.close();
			/* code to use values read from the file */
		} catch (Exception e) {
			Logger.logInfoMessage("Failed to read from " + PROP_FILE + " file.");
			e.printStackTrace();
			Logger.logInfoMessage("Shutting down client.....");
			System.exit(1);

		}


		Logger.logInfoMessage("client.TPS: " + client_tps);
		Logger.logInfoMessage("client.maxtime: " + client_maxtime);
		Logger.logInfoMessage("client.producer.payload.size.bytes: "
				+ payload_size);
		Logger.logInfoMessage("client.producer.payload.file: " + payload_file);
		Logger.logInfoMessage("client.producer.payload.dir: " + payload_dir);
		Logger.logInfoMessage("client.producer.payload.fileprefix: " + payload_fileprefix);
//		Logger.logInfoMessage("client.producer.payload.idfileprefix: " + id_fileprefix);
		
		Logger.logInfoMessage("client.producer.threads: " + threads_produce);
		Logger.logInfoMessage("client.listener.threads: " + threads_consume);
		Logger.logInfoMessage("client.listener.timeout.millis: " + listen_timeout);

		Logger.logInfoMessage("stats.file.enabled: " + file_stats);
		Logger.logInfoMessage("stats.file.name: " + stats_file_name);
		Logger.logInfoMessage("stats.interval.millis: " + stats_interval_ms);
		Logger.logInfoMessage("kafka.bootstrap.servers: " + kafka_bootstrap_servers);
		Logger.logInfoMessage("kafka.topic: " + kafka_topic);
		Logger.logInfoMessage("kafka.compressiontype: " + kafka_compressiontype);
		Logger.logInfoMessage("kafka.listener.groupName: " + kafka_listener_groupName);
		Logger.logInfoMessage("kafka.listener.maxpollrecords: " + kafka_listener_maxpollrecords);
		
		Logger.logInfoMessage("client.producer.payload.eventid.min:" + eventid_min);
		Logger.logInfoMessage("client.producer.payload.eventid.max: " + eventid_max);
		Logger.logInfoMessage("client.producer.payload.marketid.prefix.min: " + marketid_prefix_min);
		Logger.logInfoMessage("client.producer.payload.marketid.prefix.max: " + marketid_prefix_max);
		Logger.logInfoMessage("client.producer.payload.selectionid.prefix.min: " + selectionid_prefix_min);
		Logger.logInfoMessage("client.producer.payload.selectionid.prefix.max: " + selectionid_prefix_max);
		Logger.logInfoMessage("client.producer.payload.selection.priceupdate.multiplier: " + selection_priceupdate_multiplier);
		Logger.logInfoMessage("client.producer.inplaytime: " + inplay_seconds);
		Logger.logInfoMessage("client.producer.resulttime: " + result_seconds);
		Logger.logInfoMessage("TopLevelList: " + topLevelList);

	}
	private static void addParentString(String topLevels){
		String[] parents = topLevels.split("-");
		if (parents != null && parents.length == 3){
			topLevelList.addLast("c."+parents[0]+":cl."+parents[1]+":t."+parents[2]);
		}
	}
}
