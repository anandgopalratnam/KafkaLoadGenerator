package com.lc.df.kafka.client;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Properties;
import java.util.concurrent.CountDownLatch;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.log4j.Logger;

public class KafkaClient implements Runnable
{
    // parameters
    private String kafkaBootstrapServers = KafkaClientConfig.kafka_bootstrap_servers;
    private String compressionType = KafkaClientConfig.kafka_compressiontype;
    private int threads = KafkaClientConfig.threads_produce;

    // variables

    private int sentCount;
    private long startTime;
    private long endTime;
    private long elapsed;
    private List<KafkaProducer<String, String>> producerList = new ArrayList<KafkaProducer<String,String>>();
    private List<KafkaMsgConsumer> consumerList = new ArrayList<KafkaMsgConsumer>();
    

	private CountDownLatch startSignal;
	private CountDownLatch doneSignal;

	private static Logger log;
	public static int total_discarded = 0,total_error =0,total_error_connect = 0,total_error_timeout=0,total_error_unknown =0,total_sent=0,total_success=0,sent_last_round=0,fileIndex =0;
	public static long startSending = 0,stopSending = 0 ,sumLatency =0,avg_latency=0, tps_avg = 0,tps_last =0,sending_time =0; 


	public KafkaClient(Logger logger)
	{
		log = logger;
	}

	public static Logger getLogger()
	{
	   return log;
	}

	public void run()
	{
		try
		{
			prepareConnectivity();

			prepareConfig();
			
			prepareListeners();

			prepareProducers();

			execute();
	 	}
	 	catch(Exception e)
	 	{
			e.printStackTrace();
		}

		com.lc.df.kafka.client.Logger.logInfoMessage("Printing Final Stats: ........" + Kpi.getStats(true));
	}

	private void prepareConnectivity() throws Exception
	{
        for (int i=0;i<threads;i++)
        {
    	    Properties props = new Properties();
    	    props.put("bootstrap.servers", kafkaBootstrapServers);
    	    props.put("acks", "1");
    	    props.put("retries", 1);
    	    props.put("batch.size", 1048576);
    	    props.put("linger.ms", 1);
    	    props.put("buffer.memory", 33554432L);
    	    props.put("buffer.memory", 33554432L);
    	    props.put("compression.type", compressionType);
    	    props.put("max.in.flight.requests.per.connection", "1");
    	    props.put("key.serializer", "org.apache.kafka.common.serialization.StringSerializer");
    	    props.put("value.serializer", "org.apache.kafka.common.serialization.StringSerializer");
    	    producerList.add(new KafkaProducer<String,String>(props));
     
    	}
	}
	private void prepareConfig()
	{
		Utils.loadPayloadAndIDs();
	}
	private void prepareProducers() throws Exception
	{
		startSignal = new CountDownLatch(1);
		doneSignal = new CountDownLatch(threads);

		for (int i=0; i<threads; ++i) // create and start threads
			new Thread(new KafkaMsgSender(	i,
											startSignal,
											doneSignal,
											producerList.get(i))).start();
	}

	private void execute() throws Exception
	{
		startSignal.countDown();      // let all threads proceed
		doneSignal.await();           // wait for all to finish

		try{
			Thread.sleep(KafkaClientConfig.listen_timeout);
		}
		catch(Exception e)
		{
			e.printStackTrace();
		}
		Kpi.stopSending();
		com.lc.df.kafka.client.Logger.logInfoMessage("(kafkaClient) releasing resources.");

        System.err.println(getPerformance());
    	Kpi.writeReport();
	}

	void prepareListeners() throws Exception
	{

		for(int i=0;i<KafkaClientConfig.threads_consume;i++)
		{
			KafkaMsgConsumer consumer = new KafkaMsgConsumer(i);
			consumerList.add(consumer);
			consumer.start();
		}
		com.lc.df.kafka.client.Logger.logInfoMessage("Sleeping for 5 seconds to allow consumers to initialise........");
		Thread.sleep(5000);
	}


    /**
     * Get the performance results.
     */
    private String getPerformance()
    {
        if (endTime > startTime)
        {
            elapsed = endTime - startTime;
            double seconds = elapsed/1000.0;
            int perf = (int)((sentCount * 1000.0)/elapsed);
            return (sentCount + " times took " + seconds + " seconds, performance is " + perf + " messages/second");
        }
        else
        {
            return "interval too short to calculate a message rate";
        }
    }

    /**
     * Get the total elapsed time.
     */
    public long getElapsedTime()
    {
        return elapsed;
    }

    /**
     * Get the total produced message count.
     */
    public int getSentCount()
    {
        return sentCount;
    }


	private static void resetStats() {
		
		Kpi.total_discarded = 0;
		Kpi.total_error = 0;
		Kpi.total_error_connect = 0;
		Kpi.total_error_timeout = 0;
		Kpi.total_error_unknown = 0;
		Kpi.total_sent = 0;
		Kpi.messageTypeCount = new HashMap<String, Integer>();
		Kpi.total_success = 0;
		Kpi.startSending = KafkaMsgSender.start_Time;
		Kpi.stopSending = KafkaMsgSender.end_Time;
		Kpi.sumLatency = 0;
		Kpi.sent_last_round = 0;
		Kpi.fileIndex = 1;
		Kpi.avg_latency = 0;
		Kpi.outFileName = null;
	}
	
    /**
     * main
     */
    public static void main(String[] args)
    {
    	final long startTime = System.currentTimeMillis();
		Runtime.getRuntime().addShutdownHook(new Thread() {
			public void run()
			{
				long avg_latency = 0;
				if(total_success != 0)
					avg_latency = sumLatency / total_success;

				//calculation for average TPS rate
				double t1 = (total_sent*1000);
				double t2 = sending_time;
				double t3 = t1 / t2;
				long tps_avg = Math.round(t3);


				int stint = total_sent - sent_last_round;
				sent_last_round = total_sent;
				t1 = (stint*1000);
				t2 = KafkaClientConfig.stats_interval_ms;
				t3 = t1 / t2;
				long tps_last = Math.round(t3);
				com.lc.df.kafka.client.Logger.logInfoMessage("stats1: " + "TPS(avg):" + tps_avg + ", TPS(last):" + tps_last + ", TIME: " + sending_time + ", sent:" + total_sent + ", ok:" + total_success + ", error:" + total_error + "(c/t/d/o: " + total_error_connect + "/" + total_error_timeout + "/" + total_discarded + "/" + total_error_unknown +"), avg_latency:" + avg_latency);
			}
		 });

		for (int i = 0; i < args.length; i++) {
			KafkaClientConfig config = new KafkaClientConfig(args[i]);
			config.init();
			Logger logger = Logger.getLogger("ClientLogger");
			KafkaClient client = new KafkaClient(logger);
			new Thread(new Runnable() {
				
				@Override
				public void run() {
					while (true) {
						long elapsedTime = System.currentTimeMillis() - startTime;
						if (elapsedTime > (KafkaClientConfig.result_seconds * 1000)) {
							Utils.startResults();
						} else if (elapsedTime > (KafkaClientConfig.inplay_seconds * 1000)) {
							Utils.startInplay();
						} 
						try {
							Thread.sleep(1000);
						} catch (InterruptedException e) {
							// TODO Auto-generated catch block
							e.printStackTrace();
						}
					}
				}
			}).start();
			client.run();
			resetStats();
		}
		System.exit(0);
    }

}
