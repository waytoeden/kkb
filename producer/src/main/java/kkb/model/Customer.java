package kkb.model;

import java.util.Date;

public class Customer
{
	protected long cuid = 0;
	protected String name = "";
	protected Date regDate = new Date();
	
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