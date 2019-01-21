package kkb.producer;

import java.util.Properties;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.text.DateFormat;
import java.text.SimpleDateFormat;
import java.time.LocalDateTime;
import java.util.Date;
import java.util.LinkedList;


import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.StringSerializer;

public class App 
{
	public static Date firstRegCustomerTime; 
	public static Date firstOpenAccountTime; 
	public static Date firstDepositTime; 
	public static Date firstWithdrawTime;
	public static Date firstRemitTime;
	
	
	public static ConcurrentLinkedQueue<Pair<LocalDateTime, Pair<String, Object>>> eventIntervalGen;
	
	public static ConcurrentLinkedQueue<Customer> regCustomerEventQueue; 
	public static ConcurrentLinkedQueue<OpenAccountEventStruct> openAccountEventQueue; 
	public static ConcurrentLinkedQueue<DepositEventStruct> depositEventQueue; 
	public static ConcurrentLinkedQueue<WithdrawEventStruct> withdrawEventQueue; 
	public static ConcurrentLinkedQueue<RemitEventStruct> remitEventQueue;
	
	public static boolean StopFlag = false;
	public static Customer lastCustomer;
	
	public static void main( String[] args )
	{
		//for test - generating event
		
		// even
		App.eventIntervalGen = new ConcurrentLinkedQueue<>();
		
		// ready concurrent data struct
		App.regCustomerEventQueue = new ConcurrentLinkedQueue<>(); 
		App.openAccountEventQueue = new ConcurrentLinkedQueue<>(); 
		App.depositEventQueue = new ConcurrentLinkedQueue<>(); 
		App.withdrawEventQueue = new ConcurrentLinkedQueue<>(); 
		App.remitEventQueue = new ConcurrentLinkedQueue<>();
		
		//
		int testCustomerCount = 50000;
		for(int i = 0;i < testCustomerCount;++i)
		{
			Customer c = new Customer(i,String.format("Customer_%d", i),new Date());
			LocalDateTime d = LocalDateTime.now();
			App.eventIntervalGen.add(new Pair<>(d, new Pair<>("RegCustomer", (Object)c)));
			lastCustomer = c;
		}
		System.out.println(App.eventIntervalGen.size());
		LinkedList<Thread> threadList = new LinkedList<Thread>();
		
		EventIntvThread eventIntvThread = new EventIntvThread();
		threadList.add(eventIntvThread);
		
		int threadCount = 10;
		for(int i = 0 ; i < threadCount; ++i)
		{
			TestLogThread thr = new TestLogThread();
			thr.id = i;
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
		System.out.println("Test finish");
	}
}


class EventIntvThread extends Thread
{
	public int id;
	@Override
	public void run()
	{
		while(true)
		{
			if(App.StopFlag)	// 테스테 레벨이므로 뭔가 하나라도 문제가 생기면 중단하자 
				break;
			
			if(App.eventIntervalGen.size() < 1)
			{
				try {
					Thread.sleep(0);	// 강제로 thread yield
	            } catch (InterruptedException e) {
	            	e.printStackTrace();
	            	App.StopFlag = true;
	            }
				continue;
			}
			
			// 시간별로 정렬이 되어 있을 테니 처음꺼 하나만 꺼내오면 됨
			Pair<LocalDateTime, Pair<String, Object>> firstEle = App.eventIntervalGen.peek();
			
			if(firstEle == null)
			{
				try {
					Thread.sleep(0);	// 강제로 thread yield
	            } catch (InterruptedException e) {
	            	e.printStackTrace();
	            	App.StopFlag = true;
	            }
				continue;
			}
			
			// 발생해야할 이벤트 하나를 꺼내와서 인터벌 1 min 을 추가하여 다시 event map에 넣어 주자
			if(LocalDateTime.now().isAfter(firstEle.first))
			{
				firstEle = App.eventIntervalGen.poll();
				Pair<String, Object> procEvent = firstEle.second;
				switch(procEvent.first)
				{
				case "RegCustomer":
					Customer c = (Customer)procEvent.second;
					App.regCustomerEventQueue.add(c);
					break;
				case "AccountOpen":
					OpenAccountEventStruct openAccountEvent = (OpenAccountEventStruct)procEvent.second;
					App.openAccountEventQueue.add(openAccountEvent);
					break;
				case "Deposit":
					DepositEventStruct depositEvent = (DepositEventStruct)procEvent.second;
					App.depositEventQueue.add(depositEvent);
					break;
				case "Withdraw":
					WithdrawEventStruct withdrawEvent = (WithdrawEventStruct)procEvent.second;
					App.withdrawEventQueue.add(withdrawEvent);
					break;
				case "Remit":
					RemitEventStruct remitEvent = (RemitEventStruct)procEvent.second;
					App.remitEventQueue.add(remitEvent);
					break;
				default:
					break;
				}
			}
					
			try {
				Thread.sleep(0);	// 강제로 thread yield
            } catch (InterruptedException e) {
            	e.printStackTrace();
            	App.StopFlag = true;
            }	
			
		}
	}
}


class TestLogThread extends Thread
{
	public int id;
	@Override
	public void run()
	{
		while(true)
		{
			if(App.StopFlag)	// 테스테 레벨이므로 뭔가 하나라도 문제가 생기면 중단하자 
				break;
			Customer currentCustomer = null;
			try {
				Thread.sleep(0);	// 강제로 thread yield
            } catch (InterruptedException e) {
            	e.printStackTrace();
            	App.StopFlag = true;
            }
			
			do
			{
				Customer customer = App.regCustomerEventQueue.poll();
				if(customer == null)
					break;
				BankLog.getInstance().regCustomer(customer);
				LocalDateTime d = LocalDateTime.now().plusMinutes(1);	// 1분뒤 다음 프로세스가 진행
				App.eventIntervalGen.add(new Pair<>(d, new Pair<String, Object>("AccountOpen", new OpenAccountEventStruct(customer, new Account(this.id,this.id*100, new Date())))));
			} while(false);
			
			do
			{
				OpenAccountEventStruct openAccountEvent = App.openAccountEventQueue.poll();
				if(openAccountEvent == null)
					break;
				BankLog.getInstance().openAccount(openAccountEvent.customer, openAccountEvent.account);
				LocalDateTime d = LocalDateTime.now().plusMinutes(1);	// 1분뒤 다음 프로세스가 진행
				
				// customer cuid 와 thread id 값을 조합하여 amount 값을 구성해서 문제를 추적해보자
				App.eventIntervalGen.add(new Pair<>(d, new Pair<String, Object>("Deposit", new DepositEventStruct(openAccountEvent.customer, openAccountEvent.account, this.id*1000000+openAccountEvent.customer.cuid, new Date()))));
			} while(false);

			do
			{
				DepositEventStruct depositEvent = App.depositEventQueue.poll();
				if(depositEvent == null)
					break;
				BankLog.getInstance().deposit(depositEvent.fromCustomer, depositEvent.toAccount, depositEvent.amount, depositEvent.curDate);
				LocalDateTime d = LocalDateTime.now().plusMinutes(1);	// 1분뒤 다음 프로세스가 진행
				App.eventIntervalGen.add(new Pair<>(d, new Pair<String, Object>("Withdraw", new WithdrawEventStruct(depositEvent.fromCustomer, depositEvent.toAccount, depositEvent.amount, new Date()) )));
			} while(false);
			
			do
			{
				WithdrawEventStruct withdrawEvent = App.withdrawEventQueue.poll();
				if(withdrawEvent == null)
					break;
				BankLog.getInstance().withdraw(withdrawEvent.toCustomer, withdrawEvent.fromAccount, withdrawEvent.amount, withdrawEvent.curDate);
				LocalDateTime d = LocalDateTime.now().plusMinutes(1);	// 1분뒤 다음 프로세스가 진행
				App.eventIntervalGen.add(new Pair<>(d, new Pair<String, Object>("Remit", new RemitEventStruct(withdrawEvent.toCustomer, withdrawEvent.fromAccount, "bank", this.id*1000000+withdrawEvent.toCustomer.cuid, "toAccountOwnerName", withdrawEvent.amount, new Date()) )));
			
				currentCustomer = withdrawEvent.toCustomer; 
				
			} while(false);
			
			if(App.lastCustomer == currentCustomer)
				App.StopFlag = true;
			
			try {
				Thread.sleep(0);	// 강제로 thread yield
            } catch (InterruptedException e) {
            	e.printStackTrace();
            }	
			
		}
	}
}

class Customer
{
	protected long cuid;
	protected String name;
	protected Date regDate;
	
	public Customer() {}
	public Customer(long cuid, String name, Date regDate)
	{
		this.cuid = cuid;
		this.name = name;
		this.regDate = regDate;
	}
	public long getCuid() {return this.cuid;}
	public String getName() {return this.name;}
	public Date getRegDate() {return this.regDate;}
}
class Account
{
	
	protected long no;
	protected long cuid;
	protected Date regDate;
	
	public Account() {}
	public Account(long cuid, long no, Date regDate)
	{
		this.cuid = cuid;
		this.no = no;
		this.regDate = regDate;
	}
	public long getNo() {return this.no;}
	public long getCuid() {return this.cuid;}
	public Date getRegDate() {return this.regDate;}
}

class BankLog
{
	private BankLog() {}
	private static BankLog instance;
	private KafkaProducer<String, String> producer;
	private DateFormat dateFormat = new SimpleDateFormat("yyyy-mm-dd hh:mm:ss");
	public boolean initLog(/*get config*/) 
	{
		Properties producerProps = new Properties();
		
		producerProps.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092");
		producerProps.put(ProducerConfig.ACKS_CONFIG, "all");
		producerProps.put(ProducerConfig.ENABLE_IDEMPOTENCE_CONFIG , true);
		producerProps.put(ProducerConfig.TRANSACTIONAL_ID_CONFIG, "serverID1");
		producerProps.put(ProducerConfig.CLIENT_ID_CONFIG, "producer1" );
		producerProps.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
		producerProps.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
		this.producer = new KafkaProducer<>(producerProps);
		this.producer.initTransactions();
		return true;
	}
	
	public static BankLog getInstance()
	{
		if(instance == null)
		{
			instance = new BankLog();
			instance.initLog();
		}
			
		
		return instance;
	}
	
	public void regCustomer(Customer customer) 
	{
		// KafkaProducer is thread-safe so doesn't need a lock
		String v = String.format("{'cuid':'%d', 'name':'%s', 'regDate':'%s'}", customer.getCuid(), customer.getName(), dateFormat.format(customer.getRegDate()));
		System.out.println(v);
		if(v != null)
			return;
		
		// cuid를 key로 지정하면 특정 유저의 정보는 특정 파티션에만 들어갈 테니 다른 파티션에서 발생한 로그를 걱정할 필요 없음 
		ProducerRecord<String, String> recode = new ProducerRecord<String, String>("RegCustomer", Long.toString(customer.getCuid()) , v );
		this.producer.send(recode);
		this.producer.flush();
	}
	
	//Customer와 Account 의 관계가 1:1인지 알수 없고 부모 자식의 관계인지도 알수 없기 때문에 같은 레벨의 entity로 가정함
	public void openAccount(Customer customer, Account account) 
	{
		// KafkaProducer is thread-safe so doesn't need a lock
		String v = String.format("{'no':'%d', 'cuid':'%d', 'regDate':'%s'}", account.getNo(), customer.getCuid(), dateFormat.format(account.getRegDate()));
		System.out.println(v); 
		if(v != null)
			return;
		this.producer.send(new ProducerRecord<String, String>("AccountOpen", v));
		this.producer.flush();
	}
	
	// 입금을 실행하는 고객과 입금을 받는 계좌의 관계가 없을 수도 있다는 가정
	public void deposit(Customer fromCustomer, Account toAccount, long amount, Date curDate)
	{
		String v = String.format("{'cuid':'%d', 'accountNo':'%d', 'amount':'%d', 'regDate':'%s'}", fromCustomer.getCuid(), toAccount.getNo(), amount, dateFormat.format(curDate));
		System.out.println(v); 
		if(v != null)
			return;
		this.producer.send(new ProducerRecord<String, String>("Deposit", v));
		this.producer.flush();
	}
	
	// 출금을 실행하는 고객과 출금을 당하는 계좌의 관계가 없을 수도 있다는 가정
	public void withdraw(Customer toCustomer, Account fromAccount, long amount, Date curDate)
	{
		String v = String.format("{'cuid':'%d', 'accountNo':'%d', 'amount':'%d', 'regDate':'%s'}", toCustomer.getCuid(), fromAccount.getNo(), amount, dateFormat.format(curDate));
		System.out.println(v);
		if(v != null)
			return;
		this.producer.send(new ProducerRecord<String, String>("Withdraw", v));
		this.producer.flush();
	}
	
	// 이체를 실행하는 고객과 출금을 당하는 계좌의 관계가 없을 수도 있다는 가정
	public void remit(Customer fromCustomer, Account fromAccount, String toBank, long toAccountNo, String toAccountOwnerName, long remittance, Date curDate)
	{
		String v = String.format("{'fromCustomerId':'%d', 'fromAccountNo':'%d', 'toBank':'%s', 'toAccountNo':'%d', 'toAccountOwnerName':'%s', 'remittance':'%d', 'curDate':'%d'}"
			, fromCustomer.getCuid(), fromAccount.getNo(), toBank, toAccountNo, toAccountOwnerName, remittance, dateFormat.format(curDate));
		System.out.println(v); 
		if(v != null)
			return;
		this.producer.send(new ProducerRecord<String, String>("Remit", v));
		this.producer.flush();
	}
}



class OpenAccountEventStruct
{
	public Customer customer;
	public Account account;
	public OpenAccountEventStruct(Customer customer, Account account)
	{
		this.customer = customer;
		this.account = account;
	}
	
}
class DepositEventStruct
{
	public Customer fromCustomer;
	public Account toAccount;
	public long amount;
	public Date curDate;
	public DepositEventStruct(Customer fromCustomer, Account toAccount, long amount, Date curDate)
	{
		this.fromCustomer = fromCustomer;
		this.toAccount = toAccount;
		this.amount = amount;
		this.curDate = curDate;
	}
}
class WithdrawEventStruct
{
	public Customer toCustomer;
	public Account fromAccount;
	public long amount;
	public Date curDate;
	public WithdrawEventStruct(Customer toCustomer, Account fromAccount, long amount, Date curDate)
	{
		this.toCustomer = toCustomer;
		this.fromAccount = fromAccount;
		this.amount = amount;
		this.curDate = curDate;
	}
}
class RemitEventStruct
{
	public Customer fromCustomer;
	public Account fromAccount;
	public String toBank;
	public long toAccountNo;
	public String toAccountOwnerName;
	public long remittance;
	public Date curDate;
	public RemitEventStruct(Customer fromCustomer, Account fromAccount, String toBank, long toAccountNo, String toAccountOwnerName, long remittance, Date curDate)
	{
		this.fromCustomer = fromCustomer;
		this.fromAccount = fromAccount;
		this.toBank = toBank;
		this.toAccountNo = toAccountNo;
		this.toAccountOwnerName = toAccountOwnerName;
		this.remittance = remittance;
		this.curDate = curDate;
	}
}


		






















