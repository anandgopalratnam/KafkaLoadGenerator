package com.lc.df.kafka.client;

import java.util.UUID;
import java.util.concurrent.CountDownLatch;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;

public class KafkaMsgSender extends Thread {
	private int id;

	private final CountDownLatch startSignal;
	private final CountDownLatch doneSignal;

	private KafkaProducer<String, String> producer;
	private Kpi stats;
	private TransactionContext context;
	public static long start_Time = 0;
	public static long end_Time = 0;
	

	KafkaMsgSender(int id, CountDownLatch startSignal, CountDownLatch doneSignal,
			KafkaProducer<String, String> producer) {
		this.id = id;

		this.startSignal = startSignal;
		this.doneSignal = doneSignal;
		this.producer = producer;
	}

	private synchronized void preparePayload() 
	{
		Utils.loadPayloadAndIDs();
	}

	public void run() 
	{
		try 
		{
			if (Utils.payloadListSize == 0)
			{
				preparePayload();
			}
			startSignal.await();
			int tps = KafkaClientConfig.client_tps;
			int max = KafkaClientConfig.client_maxtime * tps;
//			int interval = 1000 / tps;
			int myMax = max / KafkaClientConfig.threads_produce;
			int myTPS = KafkaClientConfig.client_tps / KafkaClientConfig.threads_produce;

			if (myTPS == 0) {
				Logger.logInfoMessage("WARNING: TPS per thread below 0, defaulting to 1 per thread.");
				myTPS = 1;
			}

			long working_window = 1000;
			long sleeping_window;

			long wait4slot = id * 1000 / (KafkaClientConfig.threads_produce);
			Thread.sleep(wait4slot);

			Logger.logInfoMessage("Sender " + id + " started, max: " + myMax);

			KafkaMsgSender.start_Time = Kpi.startSending();

			long start = System.currentTimeMillis();
			long spent;
			int count = 0;
			int countTotal = 0;

			while (countTotal < myMax) 
			{
				String newPayload = "";
				String eventKey = "";
				String corrid = UUID.randomUUID().toString();
				KafkaPayload pl = Utils.getRandomPayload(corrid);
				if (pl != null)
				{
					eventKey = pl.getKey();
					newPayload = pl.getValue();
				}
				if ("STOP".equals(eventKey) || "STOP".equals(newPayload)){
					Logger.logInfoMessage("Resulting complete... Stopping Test...");
					break;
				}
				context = new TransactionContext(corrid,System.currentTimeMillis());
				stats = context.getKpi();

				sendRequest(eventKey, newPayload);

				stats.setSent(pl.getType());

				spent = System.currentTimeMillis() - start;
				count++;
				countTotal++;

				if (count == myTPS || spent >= working_window) 
				{
					if (spent < working_window) {
						sleeping_window = 1000 - spent;
						Thread.sleep(sleeping_window);
					} else
						Logger.logInfoMessage(
								"WARNING: thread " + id + " could not maintain TPS rate (" + count + "/" + myTPS + ")");

					count = 0;
					start = System.currentTimeMillis();
				}
			}

			KafkaMsgSender.end_Time = System.currentTimeMillis();
			doneSignal.countDown();
		} catch (Exception e) {
			Logger.logInfoMessage("sending process got interrupted !");
			e.printStackTrace();
		}

		Logger.logInfoMessage("Sender " + id + " finished.");
	}

	private void sendRequest(String key, String msg) throws Exception 
	{
		producer.send(new ProducerRecord<>(KafkaClientConfig.kafka_topic,key, msg));
	}

}
