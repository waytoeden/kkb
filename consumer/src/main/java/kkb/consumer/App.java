package kkb.consumer;

import java.util.Arrays;
import java.util.HashMap;
import java.util.LinkedList;
import java.util.Map;
import java.util.Properties;
import java.util.concurrent.ConcurrentHashMap;
import java.time.Duration;

import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.ObjectMapper;

import spark.Request;
import spark.Response;
import spark.Route;
import spark.Spark;

import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.ConsumerRecord;


public class App 
{
	public static boolean StopFlag = false;
	
	public static Map<Long, String> customerProfile = new ConcurrentHashMap<>();; 
	public static Properties consumerProps;
    public static void main( String[] args )
    {
    	 
    	Spark.setPort(8091);
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
        consumerProps.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, "13.124.86.241:9092");
        consumerProps.put(ConsumerConfig.GROUP_ID_CONFIG, "gg1");
        consumerProps.put(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, "true");
        consumerProps.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
        consumerProps.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, "org.apache.kafka.common.serialization.StringDeserializer");
        consumerProps.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, "org.apache.kafka.common.serialization.StringDeserializer");
        
		
        LinkedList<Thread> threadList = new LinkedList<Thread>();
        
        int consumerCount = 1;
		for(int i = 0 ; i < consumerCount; ++i)
		{
			ConsumerThread thr = new ConsumerThread();
			thr.id = i;
			thr.consumer = new KafkaConsumer<>(consumerProps);
			thr.consumer.subscribe(Arrays.asList("RegCustomer"));
			thr.consumer.seekToBeginning(thr.consumer.assignment());
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
			ConsumerRecords<String, String> records = consumer.poll(Duration.ofMillis(500));
			
			for (ConsumerRecord<String, String> record : records) {
				try {
					
					ObjectMapper mapper = new ObjectMapper();
					Map<String, Object> map = new HashMap<String, Object>();
					
					map = mapper.readValue(record.value(), new TypeReference<Map<String, String>>(){});
					
					System.out.println(map.get("cuid"));
					Long cuid = Long.parseLong(map.get("cuid").toString());
					App.customerProfile.put(cuid, record.value());
					System.out.println(record.value());
				}
				catch(Exception e)
				{
					//
					System.out.println(e.getMessage());
				}
			}
			
		}
	}
}


