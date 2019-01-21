package kkb.consumer;

import java.util.Collections;
import java.util.LinkedList;
import java.util.Properties;
import java.util.concurrent.ConcurrentMap;
import java.text.DateFormat;
import java.text.SimpleDateFormat;
import java.time.Duration;

import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.common.serialization.StringSerializer;

import spark.Request;
import spark.Response;
import spark.Route;
import spark.Spark;

import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.ConsumerRecord;

import static spark.Spark.*;

public class App 
{
	public static boolean StopFlag = false;
	
	public static ConcurrentMap<Long, String> customerProfile; 
	
    public static void main( String[] args )
    {
    	
    	Spark.setPort(8080);
    	Spark.get(new Route("/") {
    		@Override
    		public Object handle(Request request, Response response)
    		{
    			return "hello world";
    		}
    	});
    	
    	Spark.get(new Route("/customer/:cuid") {
    		@Override
    		public Object handle(Request request, Response response)
    		{
    			String cuidStr = request.params(":cuid");
    			Long cuid = Long.parseLong(cuidStr);
    			String profile = App.customerProfile.get(cuid);
    			return (profile == null )?"":profile;
    		}
    	});
    	
        Properties consumerProps = new Properties();
        consumerProps.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092");
        consumerProps.put(ConsumerConfig.GROUP_ID_CONFIG, "ConsumerGroup_CustomerProfiler");
        consumerProps.put(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, "true");
        consumerProps.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
        consumerProps.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        consumerProps.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
		
        String topicStr = "RegCustomer";
        
        LinkedList<Thread> threadList = new LinkedList<Thread>();
        
        
        
        
        int consumerCount = 16;
		for(int i = 0 ; i < consumerCount; ++i)
		{
			ConsumerThread thr = new ConsumerThread();
			thr.id = i;
			thr.consumer = new KafkaConsumer<>(consumerProps);
			thr.consumer.subscribe(Collections.singletonList(topicStr));
			threadList.add(thr);
		}
		for (Thread thr : threadList) {
			thr.start();
		}
		for (Thread thr : threadList) {
			try {
				thr.join();
			}
			catch(Exception e)
			{
				StopFlag = true;
			}
			
		}
    }
}


class ConsumerThread extends Thread
{
	public int id;
	public KafkaConsumer<String, String> consumer;
	@Override
	public void run()
	{
		while(true)
		{
			if(App.StopFlag)
				break;
			ConsumerRecords<String, String> records = consumer.poll(Duration.ofMillis(100));
			if(records == null || records.count() < 1)
			{
				try {
					Thread.sleep(0);	// 강제로 thread yield
	            } catch (InterruptedException e) {
	            	e.printStackTrace();
	            	App.StopFlag = true;
	            }	
				continue;
			}
			
			for (ConsumerRecord<String, String> record : records) {
				Long cuid = Long.parseLong(record.key());
				App.customerProfile.put(cuid, record.value());
			}
		}
	}
}


class RestfullAPIThread extends Thread
{
	@Override
	public void run()
	{
	
	}
}











