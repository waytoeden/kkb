package kkb.producer;

import java.text.DateFormat;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.Properties;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;

import kkb.model.*;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;

public class BankLog
{
	private BankLog() {}
	private static BankLog instance;
	private static Lock _mutex = new ReentrantLock(true);
	private KafkaProducer<String, String> producer;
	private Properties producerProps;
	private DateFormat dateFormat = new SimpleDateFormat("yyyy-mm-dd hh:mm:ss");
	public boolean initLog(/*get external config*/) 
	{
		producerProps = new Properties();
		producerProps.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, "000.000.000.000:9092");
		producerProps.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, "org.apache.kafka.common.serialization.StringSerializer");
		producerProps.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, "org.apache.kafka.common.serialization.StringSerializer");

		this.producer = new KafkaProducer<>(producerProps);
		System.out.println("producer Init Success");
		return true;
	}
	
	public static BankLog getInstance()
	{
		if(instance == null)
		{
			_mutex.lock();
			if(instance == null)
			{
				instance = new BankLog();
				instance.initLog();
			}
			_mutex.unlock();
		}
			
		
		return instance;
	}
	
	public RecordMetadata regCustomer(Customer customer) 
	{
		// KafkaProducer is thread-safe
		String v = String.format("{\"cuid\":\"%d\", \"name\":\"%s\", \"regDate\":\"%s\"}", customer.getCuid(), customer.getName(), dateFormat.format(customer.getRegDate()));
		System.out.println(v);
		
		// cuid를 key로 지정하면 특정 유저의 정보는 특정 파티션에만 들어갈 테니 다른 파티션에서 발생한 로그와 순서가 뒤바뀌는 걱정을 할 필요 없는듯
		ProducerRecord<String, String> recode = new ProducerRecord<String, String>("RegCustomer", Long.toString(customer.getCuid()) , v );
		try 
		{
			RecordMetadata recordMetadata = this.producer.send(recode).get();
			this.producer.flush();
			return recordMetadata;
		}
		catch(Exception e)
		{
			e.printStackTrace();
			return null;
		}
	}
	
	//Customer와 Account 의 관계가 1:1인지 알수 없고 부모 자식의 관계인지도 알수 없기 때문에 같은 레벨의 entity로 가정함
	public RecordMetadata openAccount(Customer customer, Account account) 
	{
		// KafkaProducer is thread-safe
		String v = String.format("{\"no\":\"%d\", \"cuid\":\"%d\", \"regDate\":\"%s\"}", account.getNo(), customer.getCuid(), dateFormat.format(account.getRegDate()));
		try 
		{
			RecordMetadata recordMetadata = this.producer.send(new ProducerRecord<String, String>("AccountOpen", v)).get();
			this.producer.flush();
			return recordMetadata;
		}
		catch(Exception e)
		{
			e.printStackTrace();
			return null;
		}
	}
	
	// 입금을 실행하는 고객과 입금을 받는 계좌의 관계가 없을 수도 있다는 가정
	public RecordMetadata deposit(Customer fromCustomer, Account toAccount, long amount, Date curDate)
	{
		String v = String.format("{\"cuid\":\"%d\", \"accountNo\":\"%d\", \"amount\":\"%d\", \"regDate\":\"%s\"}", fromCustomer.getCuid(), toAccount.getNo(), amount, dateFormat.format(curDate));
		System.out.println(v); 
		try 
		{
			RecordMetadata recordMetadata = this.producer.send(new ProducerRecord<String, String>("Deposit", v)).get();
			this.producer.flush();
			return recordMetadata;
		}
		catch(Exception e)
		{
			e.printStackTrace();
			return null;
		}
	}
	
	// 출금을 실행하는 고객과 출금을 당하는 계좌의 관계가 없을 수도 있다는 가정
	public RecordMetadata withdraw(Customer toCustomer, Account fromAccount, long amount, Date curDate)
	{
		String v = String.format("{\"cuid\":\"%d\", \"accountNo\":\"%d\", \"amount\":\"%d\", \"regDate\":\"%s\"}", toCustomer.getCuid(), fromAccount.getNo(), amount, dateFormat.format(curDate));
		try 
		{
			RecordMetadata recordMetadata = this.producer.send(new ProducerRecord<String, String>("Withdraw", v)).get();
			this.producer.flush();
			return recordMetadata;
		}
		catch(Exception e)
		{
			e.printStackTrace();
			return null;
		}
	}
	
	// 이체를 실행하는 고객과 출금을 당하는 계좌의 관계가 없을 수도 있다는 가정
	public RecordMetadata remit(Customer fromCustomer, Account fromAccount, String toBank, long toAccountNo, String toAccountOwnerName, long remittance, Date curDate)
	{
		String v = String.format("{\"fromCustomerId\":\"%d\", \"fromAccountNo\":\"%d\", \"toBank\":\"%s\", \"toAccountNo\":\"%d\", \"toAccountOwnerName\":\"%s\", \"remittance\":\"%d\", \"curDate\":\"%d\"}"
			, fromCustomer.getCuid(), fromAccount.getNo(), toBank, toAccountNo, toAccountOwnerName, remittance, dateFormat.format(curDate));
		try 
		{
			RecordMetadata recordMetadata = this.producer.send(new ProducerRecord<String, String>("Remit", v)).get();
			this.producer.flush();
			return recordMetadata;
		}
		catch(Exception e)
		{
			e.printStackTrace();
			return null;
		}
	}
}