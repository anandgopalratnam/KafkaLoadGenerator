package com.lc.df.kafka.test;

import java.util.UUID;

import com.lc.df.kafka.client.KafkaClientConfig;
import com.lc.df.kafka.client.KafkaPayload;
import com.lc.df.kafka.client.Utils;

public class KafkaTest {
	public static void main(String[] args) 
	{
		try {
			KafkaClientConfig config = new KafkaClientConfig("./config/kafkaclient.properties");
			final long startTime = System.currentTimeMillis();
			config.init();
			Utils.loadPayloadAndIDs();
			new Thread(new Runnable() {
					
				@Override
				public void run() {
					while (true) {
						long elapsedTime = System.currentTimeMillis() - startTime;
						if (elapsedTime > 60000) {
							Utils.startResults();
						} else if (elapsedTime > 30000) {
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

			System.out.println(Utils.PAYLOAD_LIST.size());
			for (int i = 0; i < 20000; i++) 
			{
				KafkaPayload payload = Utils.getRandomPayload(UUID.randomUUID().toString());
				if (payload == null){
					System.out.println("All Complete.....");
					break;
				}
				else
				{
					System.out.println("Key ["+payload.getKey()+"] Type ["+payload.getType()+"] after ["+((System.currentTimeMillis()-startTime)/1000)+"] \nValue ["+payload.getValue()+"]");
				}
				Thread.sleep(1000);
			}
			
		} catch (Exception e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
	}
}
