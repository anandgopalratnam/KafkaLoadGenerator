package com.lc.df.kafka.client;

import java.io.File;
import java.io.IOException;
import java.text.DecimalFormat;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Random;
import java.util.concurrent.ThreadLocalRandom;

import org.apache.commons.io.FileUtils;
import org.apache.commons.io.FilenameUtils;
import org.apache.commons.io.filefilter.FileFilterUtils;

public class Utils
{
	private static final Random RANDOM = new Random(System.currentTimeMillis());
	private static final int PRICE_NUM_LIMIT = 500;
	private static final int PRICE_DEN_LIMIT = 1000;
	public static final List<PayloadConfig> PAYLOAD_LIST = new ArrayList<PayloadConfig>();
	
	private static String EVT_CREATE_PAYLOAD = null;
	private static String MKT_CREATE_PAYLOAD = null;
	private static String SEL_CREATE_PAYLOAD = null;
	private static String EVT_INPLAY_PAYLOAD = null;
	private static String EVT_RESULT_PAYLOAD = null;
	private static String MKT_RESULT_PAYLOAD = null;
	private static String SEL_RESULT_PAYLOAD = null;
	
	public volatile static int payloadListSize = 0;
	private volatile static boolean createsCompleted = false; 
	private volatile static long currentEventID = KafkaClientConfig.eventid_min;
	private volatile static int currentMarketPrefixID = -1;
	private volatile static int currentSelectionPrefixID = -1;
	
	private volatile static boolean sendInplay = false;
	private volatile static boolean inplayComplete = false;
	private volatile static boolean sendResults = false;
	private volatile static boolean resultsComplete = false;
	
	private static final Map<String, String> eventParentsMap = new HashMap<String,String>();
	
	public static String readFile(String file) {
		return readFile(new File(file));
	}

	public static String readFile(File file) {
		try {
			return FileUtils.readFileToString(file);
		} catch (IOException e) {
			// TODO Auto-generated catch block
			Logger.logInfoMessage("ERROR reading life");
		}
		return null;
	}
	public static List<String> readLines(File f)
	{
		try {
			return FileUtils.readLines(f);
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		return new ArrayList<String>();
	}
	public static String getCorrID(String value)
	{
    	try 
    	{
    		String msgIDString = "messageID";
			int indexOfMsgID = value.indexOf(msgIDString);
			if (indexOfMsgID > -1)
			{
				int indexOfComma = value.indexOf(",", indexOfMsgID);
				String str = value.substring(indexOfMsgID + msgIDString.length() + 2, indexOfComma);
				return str.replaceAll("\"", "").replaceAll(":", "").trim();
			}
		} catch (Exception e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
    	return "";
	}
	public static String getNewPayload(String payload,String eventid,String marketid,String selectionid,String marketIDPrefix,String selectionIdPrefix,String messageid)
	{
		try 
		{
			String currentTimeStamp = Long.toString(System.currentTimeMillis());
			String pl = payload;
			String toplevel = eventParentsMap.get(eventid);
			if (toplevel == null){
				toplevel = "";
			}
			if (selectionid != null)
			{
				int num = getRandomIntInRange(PRICE_NUM_LIMIT);
				int den = getRandomIntInRange(PRICE_DEN_LIMIT - 1) + 1; // prevent denominator of zero
				double decimal = 1 + ((double)num/(double)den);
				DecimalFormat f = new DecimalFormat("##.00");
				pl = payload.replaceAll("\\$\\{selection\\}", selectionid)
						.replaceAll("\\$\\{market\\}", marketid)
						.replaceAll("-9999",selectionIdPrefix)
						.replaceAll("-1111", ""+num)
						.replaceAll("-2222", ""+den)
						.replaceAll("3333.33", ""+f.format(decimal));
			}
			else if (marketid != null)
			{
				pl = payload.replaceAll("\\$\\{market\\}", marketid)
							.replaceAll("-9999",marketIDPrefix);
			}
			return 	pl.replaceAll("\\$\\{toplevel\\}", toplevel)
					.replaceAll("\\$\\{event\\}", eventid)
					.replaceAll("-9999", eventid)
					.replaceAll("\\$\\{recordModifiedTime\\}", currentTimeStamp)
					.replaceAll("\\$\\{messageid\\}",messageid);

		} catch (Exception e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		return "";
	}
	
	public static int getRandomIntInRange(int range)
	{
		return RANDOM.nextInt(range);
	}
	public static void loadPayloadAndIDs()
	{
		if (KafkaClientConfig.payload_file != null && KafkaClientConfig.payload_file.length() > 0) 
		{
			String fileContent = Utils.readFile(KafkaClientConfig.payload_file);
			if (fileContent != null && fileContent.length() > 0)
			{
				PAYLOAD_LIST.add(new PayloadConfig("General",fileContent));
			}
		} else 
		{
			File dir = new File(KafkaClientConfig.payload_dir);
			
			Collection<File> payloadFiles = FileUtils.listFiles(dir,FileFilterUtils.prefixFileFilter(KafkaClientConfig.payload_fileprefix),null);
			for (File file : payloadFiles) 
			{
				String payload = Utils.readFile(file);
				String key = FilenameUtils.getBaseName(file.getName());
				if (key.contains("EventCreate"))
				{
					EVT_CREATE_PAYLOAD = payload;
				}
				if (key.contains("EventUpdate"))
				{
					PAYLOAD_LIST.add(new PayloadConfig("E",payload));
				}
				if (key.contains("EventInplay"))
				{
					EVT_INPLAY_PAYLOAD = payload;
				}
				if (key.contains("EventResult"))
				{
					EVT_RESULT_PAYLOAD = payload;
				}
				if (key.contains("MarketCreate"))
				{
					MKT_CREATE_PAYLOAD = payload;
				}
				if (key.contains("MarketUpdate"))
				{
					PAYLOAD_LIST.add(new PayloadConfig("M", payload));
				}
				if (key.contains("MarketResult"))
				{
					MKT_RESULT_PAYLOAD = payload;
				}
				if (key.contains("SelectionCreate"))
				{
					SEL_CREATE_PAYLOAD = payload;
				}
				if (key.contains("SelectionUpdate"))
				{
					if (key.contains("Price"))
					{
						for (int i = 0; i < KafkaClientConfig.selection_priceupdate_multiplier; i++) {
							PAYLOAD_LIST.add(new PayloadConfig("S",payload));
						}
					}
					else
					{
						PAYLOAD_LIST.add(new PayloadConfig("S",payload));
					}
				}
				if (key.contains("SelectionResult"))
				{
					SEL_RESULT_PAYLOAD = payload;
				}
				
			}
		}

		for (long i = KafkaClientConfig.eventid_min; i <= KafkaClientConfig.eventid_max; i++) {
			eventParentsMap.put(Long.toString(i),KafkaClientConfig.topLevelList.removeFirst());
		}
		synchronized (PAYLOAD_LIST) 
		{
			payloadListSize = PAYLOAD_LIST.size();
		}
	}

	private synchronized static KafkaPayload getCreatePayload(String correlationId)
	{
		if (createsCompleted || currentEventID > KafkaClientConfig.eventid_max)
		{
			Logger.logInfoMessage("Creates Completed.....");
			return null;
		}
		if (currentMarketPrefixID == -1 && currentSelectionPrefixID == -1)
		{
			String eventID = Long.toString(currentEventID);
			currentMarketPrefixID = KafkaClientConfig.marketid_prefix_min;
			return new KafkaPayload(eventID,"event-create",getNewPayload(EVT_CREATE_PAYLOAD, eventID, null, null,null,null, correlationId));
		}
		else if (currentMarketPrefixID > -1 && currentSelectionPrefixID == -1)
		{
			String eventID = Long.toString(currentEventID);
			String marketIDPrefixString = Integer.toString(currentMarketPrefixID);
			String marketID = Integer.toString(currentMarketPrefixID) + eventID;
			currentSelectionPrefixID = KafkaClientConfig.selectionid_prefix_min;
			return new KafkaPayload(eventID,"market-create",getNewPayload(MKT_CREATE_PAYLOAD, eventID, marketID, null,marketIDPrefixString,null, correlationId));
		}
		else if (currentMarketPrefixID > -1 && currentSelectionPrefixID > -1)
		{
			String eventID = Long.toString(currentEventID);
			String marketIDPrefixString = Integer.toString(currentMarketPrefixID);
			String marketID = marketIDPrefixString + eventID;
			String selectionIDPrefixString = Integer.toString(currentSelectionPrefixID);
			String selectionID = selectionIDPrefixString + marketID;
			currentSelectionPrefixID++;
			if (currentSelectionPrefixID > KafkaClientConfig.selectionid_prefix_max)
			{
				currentSelectionPrefixID = -1;
				currentMarketPrefixID++;
				if (currentMarketPrefixID > KafkaClientConfig.marketid_prefix_max)
				{
					currentMarketPrefixID = -1;
					currentEventID++;
				}
			}
			createsCompleted = currentEventID > KafkaClientConfig.eventid_max;
			return new KafkaPayload(eventID,"selection-create",getNewPayload(SEL_CREATE_PAYLOAD, eventID, marketID, selectionID,marketIDPrefixString,selectionIDPrefixString, correlationId));
		}
		return null;
	}
	private synchronized static KafkaPayload getInplayPayload(String correlationId)
	{
		if (inplayComplete || currentEventID > KafkaClientConfig.eventid_max)
		{
			Logger.logInfoMessage("Inplay Completed.....");
			return null;
		}
		String eventID = Long.toString(currentEventID++);
		inplayComplete = currentEventID > KafkaClientConfig.eventid_max;
		return new KafkaPayload(eventID,"event-inplay",getNewPayload(EVT_INPLAY_PAYLOAD, eventID, null, null,null,null, correlationId));
	}
	private synchronized static KafkaPayload getResultsPayload(String correlationId)
	{
		if (resultsComplete || currentEventID > KafkaClientConfig.eventid_max)
		{
			Logger.logInfoMessage("Results Completed.....");
			return null;
		}
		if (currentMarketPrefixID == -1 && currentSelectionPrefixID == -1)
		{
			String eventID = Long.toString(currentEventID++);
			if (currentEventID > KafkaClientConfig.eventid_max){
				resultsComplete = true;				
			}
			else {
				currentMarketPrefixID = KafkaClientConfig.marketid_prefix_min;
				currentSelectionPrefixID = KafkaClientConfig.selectionid_prefix_min;
			}
			return new KafkaPayload(eventID,"event-result",getNewPayload(EVT_RESULT_PAYLOAD, eventID, null, null,null,null, correlationId));
		}
		else if (currentMarketPrefixID > -1 && currentSelectionPrefixID == -1)
		{
			String eventID = Long.toString(currentEventID);
			String marketID = Integer.toString(currentMarketPrefixID++) + eventID;
			if (currentMarketPrefixID > KafkaClientConfig.marketid_prefix_max){
				currentMarketPrefixID = -1;
			}
			else{
				currentSelectionPrefixID = KafkaClientConfig.selectionid_prefix_min;
			}
			return new KafkaPayload(eventID,"market-result",getNewPayload(MKT_RESULT_PAYLOAD, eventID, marketID, null,null,null, correlationId));
		}		
		else if (currentMarketPrefixID > -1 && currentSelectionPrefixID > -1)
		{
			String eventID = Long.toString(currentEventID);
			String marketID = Integer.toString(currentMarketPrefixID) + eventID;
			String selectionID = Integer.toString(currentSelectionPrefixID) + marketID;
			currentSelectionPrefixID++;
			if (currentSelectionPrefixID > KafkaClientConfig.selectionid_prefix_max){
				currentSelectionPrefixID = -1;
			}
			return new KafkaPayload(eventID,"selection-result",getNewPayload(SEL_RESULT_PAYLOAD, eventID, marketID, selectionID,null,null, correlationId));
		}

		return null;
	}
    private static long getRandomLongId(long min, long max)
    {
        if (min < max)
        {
            return ThreadLocalRandom.current().nextLong(min, max);
        }
        else
        {
            // Handle the case where the min and the max are the same.
            return min;
        }
    }
    private static int getRandomIntId(int min, int max)
    {
        if (min < max)
        {
            return ThreadLocalRandom.current().nextInt(min, max);
        }
        else
        {
            // Handle the case where the min and the max are the same.
            return min;
        }
    }
	public static KafkaPayload getRandomPayload(String correlationId)
	{
		if (resultsComplete){
			return new KafkaPayload("STOP","STOP", "STOP");
		}
		else if (!createsCompleted) 
		{
			return getCreatePayload(correlationId);
		}
		else if (sendInplay && !inplayComplete){
			return getInplayPayload(correlationId);
		}
		else if (sendResults && !resultsComplete){
			return getResultsPayload(correlationId);
		}
		else{
			int plID = getRandomIntInRange(payloadListSize);
			PayloadConfig kPayload = PAYLOAD_LIST.get(plID >0 ? plID - 1:0);
			if (kPayload != null)
			{
				String payload = kPayload.getPayload();
				if ("E".equals(kPayload.getType()))
				{
				    long eID = getRandomLongId(KafkaClientConfig.eventid_min, KafkaClientConfig.eventid_max);
					String eventKey = Long.toString(eID);
					String newPayload = getNewPayload(payload, eventKey, null,null,null,null, correlationId);
					return new KafkaPayload(eventKey,"event-update",newPayload);
				}
				if ("M".equals(kPayload.getType()))
				{
				    long eID = getRandomLongId(KafkaClientConfig.eventid_min, KafkaClientConfig.eventid_max);
				    int mID = getRandomIntId(KafkaClientConfig.marketid_prefix_min, KafkaClientConfig.marketid_prefix_max);
					String eventKey = Long.toString(eID);
					String marketKey = Integer.toString(mID) + eventKey;
					String newPayload = getNewPayload(payload, eventKey, marketKey,null,null,null, correlationId);
					return new KafkaPayload(eventKey,"market-update",newPayload);
				}
				if ("S".equals(kPayload.getType()))
				{
				    long eID = getRandomLongId(KafkaClientConfig.eventid_min, KafkaClientConfig.eventid_max);
				    int mID = getRandomIntId(KafkaClientConfig.marketid_prefix_min, KafkaClientConfig.marketid_prefix_max);
				    int sID = getRandomIntId(KafkaClientConfig.selectionid_prefix_min, KafkaClientConfig.selectionid_prefix_max);
					String eventKey = Long.toString(eID);
					String marketKey = Integer.toString(mID) + eventKey;
					String selectionKey = Integer.toString(sID) + marketKey;
					String newPayload = getNewPayload(payload, eventKey, marketKey,selectionKey,null,null, correlationId);
					return new KafkaPayload(eventKey,"selection-update",newPayload);
				}
			}			
		}
		return null;
	}
	public static synchronized void startInplay(){
		if (!sendInplay){
			sendInplay = true;
			inplayComplete = false;
			currentEventID = KafkaClientConfig.eventid_min;
		}
	}
	public static synchronized void startResults(){
		if (!sendResults){
			sendResults = true;
			resultsComplete = false;
			currentEventID = KafkaClientConfig.eventid_min;
			currentMarketPrefixID = KafkaClientConfig.marketid_prefix_min;
			currentSelectionPrefixID = KafkaClientConfig.selectionid_prefix_min;
		}
	}
}
