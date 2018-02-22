package com.lc.df.kafka.client;

import java.util.Arrays;
import java.util.Properties;

import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;

public class KafkaMsgConsumer extends Thread
{
	private volatile boolean stop = false;
	int id;
	KafkaConsumer<String, String> consumer = null;
	public KafkaMsgConsumer(int id)
	{
		Logger.logInfoMessage("Consumer " + id + " ready!");
		initConsumer();
		new KafkaClientResponseHandler(id);
	}
	
    public synchronized boolean isStopped()
    {
    	return stop;
    }
    public synchronized void setStopped()
    {
    	this.stop = true;
    }
    private void initConsumer()
    {
		try 
		{
			Properties props = new Properties();
			props.put("bootstrap.servers",KafkaClientConfig.kafka_bootstrap_servers);
			props.put("group.id",KafkaClientConfig.kafka_listener_groupName);
			props.put("enable.auto.commit","true");
			props.put("session.timeout.ms","30000");
			props.put("max.partition.fetch.bytes", "10485760");
			props.put("max.poll.records",KafkaClientConfig.kafka_listener_maxpollrecords);
			props.put("key.deserializer","org.apache.kafka.common.serialization.StringDeserializer");
			props.put("value.deserializer","org.apache.kafka.common.serialization.StringDeserializer");
			String topicName = KafkaClientConfig.kafka_topic;
			
			consumer = new KafkaConsumer<String, String>(props);
			consumer.subscribe(Arrays.asList(topicName.split(",")));
		} catch (Exception ex) 
		{
			ex.printStackTrace();
			Logger.logInfoMessage(
					"Exception in Thread processing for KafkaSubscriber ");
		} 
    }
    //this threads controls timeout transactions
	public void run()
	{
		while (!isStopped()) 
		{
			ConsumerRecords<String, String> records = consumer.poll(Long.MAX_VALUE);
			if (records != null)
			{
				for (ConsumerRecord<String, String> record : records) 
				{
					onMessage(record.key(), record.value());
				}
			}
			try {
				Thread.sleep(2);
			} catch (InterruptedException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}
		}
	}
	   /*---------------------------------------------------------------------
     * onMessage
     *---------------------------------------------------------------------*/
    public void onMessage(String key,String value)
    {
        try
        {
//			String correlationId = key;
			String correlationId = Utils.getCorrID(value);
			TransactionContext txContext = TransactionContext.extractContext(correlationId);
			if(txContext != null)
			{
				Kpi stats = txContext.getKpi();
				stats.setReceived(value, txContext);
				if(value != null)
				{
					//Logger.logInfoMessage("Deleting............"+correlationId);
					txContext.delete();
				}
			}
			else
			{
			//	Logger.logInfoMessage("JMS response ignored: no context found for correlation id: " + correlationId);
				Kpi.setDiscarded();
			//	Logger.logInfoMessage("******** Discarded");
			}

/*
            //Logger.logInfoMessage("Received message: " + msg);
            long client_sent = msg.getLongProperty("client-sent");
            long server_rcvd = msg.getLongProperty("server-received");
            long server_resp = msg.getLongProperty("server-responded");

            long latency_server = server_resp - server_rcvd;

            kpi.setReceived(client_sent, latency_server);
*/
        }
        catch(Exception e)
        {
            Logger.logErrorMessage("Unexpected exception in the message callback!" , e);
            Logger.logInfoMessage("Exit --- 2");
            System.exit(-1);
        }
    }
}
