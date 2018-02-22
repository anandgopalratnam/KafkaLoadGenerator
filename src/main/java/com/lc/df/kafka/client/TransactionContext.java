package com.lc.df.kafka.client;

import java.util.Hashtable;
import java.util.UUID;
import java.util.concurrent.DelayQueue;
import java.util.concurrent.Delayed;
import java.util.concurrent.TimeUnit;


    /**
     * An HTTP server that sends back the content of the received HTTP request
     * in a pretty plaintext form.
     */
    public class TransactionContext implements Delayed
	{

        private long DELAY;
		protected long timestamp;
		@SuppressWarnings("unused")
		private boolean expired;

		//Unique identifiers
		@SuppressWarnings("unused")
		private UUID contextId;
		private String correlationId;
		private Kpi stats;

		//collection of all contexts pending to be completed
		static final Hashtable<String,TransactionContext> all = new Hashtable<String,TransactionContext>();
       //

		//collection of timeouts for pending transactions
		private static final DelayQueue<TransactionContext> timeoutControl = new DelayQueue<TransactionContext>();

        public TransactionContext(String correlationID,long sent )
        {
			DELAY = KafkaClientConfig.listen_timeout;
            this.timestamp = sent;
			this.expired = false;

			//we create unique identifiers and keep a copy in memory
			this.contextId = UUID.randomUUID();			
			this.correlationId =  correlationID;
			//we store the context and memory
			all.put(this.correlationId, this);
			//we set a timeout control
			timeoutControl.add(this);
			stats = new Kpi(this.correlationId);
        }
		public Kpi getKpi()
		{
			return stats;
		}

		public static DelayQueue<TransactionContext> getTimeoutControl()
		{
			return timeoutControl;
		}

		public static TransactionContext extractContext(String correlationId)
		{
			if(correlationId != null)
			{
				TransactionContext toReturn = (TransactionContext)all.get(correlationId);
				if(toReturn != null )
				{
					//Logger.logInfoMessage("Deleting in extractContext............"+correlationId);
					all.remove(correlationId);
				}
				return toReturn;
			}
			return null;
		}

		public void delete()
		{
			//all.remove(this.correlationId);
		}

		public String getCorrelationId()
		{
			return correlationId;
		}


		public long getDelay(TimeUnit unit)
		{
			return DELAY - (System.currentTimeMillis() - this.timestamp);
		}

		public void setExpired()
		{
			expired = true;
		}

		public int compareTo(Delayed other)
		{
			long comparison = ((TransactionContext)other).timestamp - this.timestamp;

			if(comparison > 0)
				return -1;
			else if(comparison < 0)
				return 1;
			else
				return 0;
		}
		public static Hashtable<String,TransactionContext> getRemainingTransactionContext()
		{
			return all;
		}
    }
