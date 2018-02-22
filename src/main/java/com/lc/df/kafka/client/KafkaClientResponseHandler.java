package com.lc.df.kafka.client;
import java.util.concurrent.DelayQueue;

public class KafkaClientResponseHandler extends Thread
{
	private volatile boolean stop = false;
	int id;

	private static DelayQueue<TransactionContext> timeoutControl = TransactionContext.getTimeoutControl();

    public KafkaClientResponseHandler(int id)
    {
		Logger.logInfoMessage("ResponseHandler " + id + " ready!");
	}

    public synchronized boolean isStopped()
    {
    	return stop;
    }
    public synchronized void setStopped()
    {
    	this.stop = true;
    }

    //this threads controls timeout transactions
	public void run()
	{
		TransactionContext context = null;
		while(!isStopped())
		{
			try
			{
				//blocks until an expired event is available
				context = (TransactionContext)timeoutControl.take();
				
				Kpi stats = context.getKpi();
				if(!stats.isSuccess())
				{
					stats.setTimeout();
				}
			}
			catch(InterruptedException e)
			{
				e.printStackTrace();
			}
		}
	}


}


