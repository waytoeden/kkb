package kkb.model;

import java.util.Date;

public class Account
{
	
	protected long no;
	protected long cuid;
	protected Date regDate;
	
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
