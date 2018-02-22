package com.lc.df.kafka.client;

public class KafkaClientStats extends Thread
{
	KafkaClientStats()
	{
	}

	public void run()
	{
		try
		{
			while(!Kpi.hasStoppedSending())
			{
				Logger.logInfoMessage("Stats: " + Kpi.getStats());
//					kpi.writeReport();
				Thread.sleep(KafkaClientConfig.stats_interval_ms);
//					Thread.sleep(10000);
			}
		}
		catch(Exception e)
		{
			Logger.logInfoMessage("tibjmsClientStats interrupted !");
			e.printStackTrace();
		}
	}
}