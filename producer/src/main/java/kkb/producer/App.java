package kkb.producer;

import java.util.concurrent.ConcurrentLinkedQueue;

import java.time.LocalDateTime;
import java.util.Date;
import java.util.LinkedList;


import kkb.producer.BankLog;
import kkb.model.*;

public class App 
{

	
	
	public static boolean StopFlag = false;
	
	
	public static void main( String[] args )
	{
		
		int testCustomerCount = 50000;
		for(int i = 0;i < testCustomerCount;++i)
		{
			Customer c = new Customer(i,String.format("Customer_%d", i),new Date());
			LocalDateTime d = LocalDateTime.now();
			TestLogThread.eventIntervalGen.add(new Pair<>(d, new Pair<>("RegCustomer", (Object)c)));
			TestLogThread.lastCustomer = c;
		}
		System.out.println(TestLogThread.eventIntervalGen.size());
		LinkedList<Thread> threadList = new LinkedList<Thread>();
		
		EventIntvThread eventIntvThread = new EventIntvThread();
		threadList.add(eventIntvThread);
		
		int threadCount = 16;
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
				e.printStackTrace();
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
			
			if(TestLogThread.eventIntervalGen.size() < 1)
			{
				try {
					Thread.sleep(0);	// 강제로 thread yield
	            } catch (InterruptedException e) {
	            	App.StopFlag = true;
	            	e.printStackTrace();
	            }
				continue;
			}
			
			// 시간별로 정렬이 되어 있을 테니 처음꺼 하나만 꺼내오면 됨
			Pair<LocalDateTime, Pair<String, Object>> firstEle = TestLogThread.eventIntervalGen.peek();
			
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
				firstEle = TestLogThread.eventIntervalGen.poll();
				Pair<String, Object> procEvent = firstEle.second;
				switch(procEvent.first)
				{
				case "RegCustomer":
					Customer c = (Customer)procEvent.second;
					TestLogThread.regCustomerEventQueue.add(c);
					break;
				case "AccountOpen":
					OpenAccountEventStruct openAccountEvent = (OpenAccountEventStruct)procEvent.second;
					TestLogThread.openAccountEventQueue.add(openAccountEvent);
					break;
				case "Deposit":
					DepositEventStruct depositEvent = (DepositEventStruct)procEvent.second;
					TestLogThread.depositEventQueue.add(depositEvent);
					break;
				case "Withdraw":
					WithdrawEventStruct withdrawEvent = (WithdrawEventStruct)procEvent.second;
					TestLogThread.withdrawEventQueue.add(withdrawEvent);
					break;
				case "Remit":
					RemitEventStruct remitEvent = (RemitEventStruct)procEvent.second;
					TestLogThread.remitEventQueue.add(remitEvent);
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
	public static ConcurrentLinkedQueue<Pair<LocalDateTime, Pair<String, Object>>> eventIntervalGen = new ConcurrentLinkedQueue<>();
	
	public static ConcurrentLinkedQueue<Customer> regCustomerEventQueue = new ConcurrentLinkedQueue<>(); 
	public static ConcurrentLinkedQueue<OpenAccountEventStruct> openAccountEventQueue = new ConcurrentLinkedQueue<>(); 
	public static ConcurrentLinkedQueue<DepositEventStruct> depositEventQueue = new ConcurrentLinkedQueue<>(); 
	public static ConcurrentLinkedQueue<WithdrawEventStruct> withdrawEventQueue = new ConcurrentLinkedQueue<>(); 
	public static ConcurrentLinkedQueue<RemitEventStruct> remitEventQueue = new ConcurrentLinkedQueue<>(); 
	
	public static Customer lastCustomer = new Customer();
	
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
				Customer customer = regCustomerEventQueue.poll();
				if(customer == null)
					break;
				BankLog.getInstance().regCustomer(customer);
				LocalDateTime d = LocalDateTime.now().plusMinutes(1);	// 1분뒤 다음 프로세스가 진행
				eventIntervalGen.add(new Pair<>(d, new Pair<String, Object>("AccountOpen", new OpenAccountEventStruct(customer, new Account(this.id,this.id*100, new Date())))));
			} while(false);
			
			do
			{
				OpenAccountEventStruct openAccountEvent = openAccountEventQueue.poll();
				if(openAccountEvent == null)
					break;
				BankLog.getInstance().openAccount(openAccountEvent.customer, openAccountEvent.account);
				LocalDateTime d = LocalDateTime.now().plusMinutes(1);	// 1분뒤 다음 프로세스가 진행
				
				// customer cuid 와 thread id 값을 조합하여 amount 값을 구성해서 문제를 추적해보자
				eventIntervalGen.add(new Pair<>(d, new Pair<String, Object>("Deposit", new DepositEventStruct(openAccountEvent.customer, openAccountEvent.account, this.id*1000000+openAccountEvent.customer.getCuid(), new Date()))));
			} while(false);

			do
			{
				DepositEventStruct depositEvent = depositEventQueue.poll();
				if(depositEvent == null)
					break;
				BankLog.getInstance().deposit(depositEvent.fromCustomer, depositEvent.toAccount, depositEvent.amount, depositEvent.curDate);
				LocalDateTime d = LocalDateTime.now().plusMinutes(1);	// 1분뒤 다음 프로세스가 진행
				eventIntervalGen.add(new Pair<>(d, new Pair<String, Object>("Withdraw", new WithdrawEventStruct(depositEvent.fromCustomer, depositEvent.toAccount, depositEvent.amount, new Date()) )));
			} while(false);
			
			do
			{
				WithdrawEventStruct withdrawEvent = withdrawEventQueue.poll();
				if(withdrawEvent == null)
					break;
				BankLog.getInstance().withdraw(withdrawEvent.toCustomer, withdrawEvent.fromAccount, withdrawEvent.amount, withdrawEvent.curDate);
				LocalDateTime d = LocalDateTime.now().plusMinutes(1);	// 1분뒤 다음 프로세스가 진행
				eventIntervalGen.add(new Pair<>(d, new Pair<String, Object>("Remit", new RemitEventStruct(withdrawEvent.toCustomer, withdrawEvent.fromAccount, "bank", this.id*1000000+withdrawEvent.toCustomer.getCuid(), "toAccountOwnerName", withdrawEvent.amount, new Date()) )));
			
				currentCustomer = withdrawEvent.toCustomer; 
				
			} while(false);
			
			if(lastCustomer == currentCustomer)
				App.StopFlag = true;
			
			try {
				Thread.sleep(0);	// 강제로 thread yield
            } catch (InterruptedException e) {
            	e.printStackTrace();
            }	
			
		}
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


		






















