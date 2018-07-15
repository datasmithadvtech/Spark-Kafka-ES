package eventum.spark_trial;

import java.io.Serializable;

public class NetflowRecordDTO implements Serializable{

	private String destinationIP;
	private String sourceIP;
	private String routerIP;
	private int inInterface;
	private int outInterface;
	private Long sumOctetsCount;
	private Long maxPacketsCount;
	public String getDestinationIP() {
		return destinationIP;
	}
	public void setDestinationIP(String destinationIP) {
		this.destinationIP = destinationIP;
	}
	public String getSourceIP() {
		return sourceIP;
	}
	public void setSourceIP(String sourceIP) {
		this.sourceIP = sourceIP;
	}
	public String getRouterIP() {
		return routerIP;
	}
	public void setRouterIP(String routerIP) {
		this.routerIP = routerIP;
	}
	public int getInInterface() {
		return inInterface;
	}
	public void setInInterface(int inInterface) {
		this.inInterface = inInterface;
	}
	public int getOutInterface() {
		return outInterface;
	}
	public void setOutInterface(int outInterface) {
		this.outInterface = outInterface;
	}
	public Long getSumOctetsCount() {
		return sumOctetsCount;
	}
	public void setSumOctetsCount(Long sumOctetsCount) {
		this.sumOctetsCount = sumOctetsCount;
	}
	public Long getMaxPacketsCount() {
		return maxPacketsCount;
	}
	public void setMaxPacketsCount(Long maxPacketsCount) {
		this.maxPacketsCount = maxPacketsCount;
	}
	
	
}
