

import java.io.Serializable;
import java.sql.Timestamp;

public class JSONOutputDTO implements Serializable {
	private Timestamp timestamp; 
	private Long recordKey;
	private String sourceIP;
	private String destinationIP;
	private String routerIP;
	private int inInterface ;
	private int outInterface;
	private Long packetsCount;
	private Long octetsCount;
	public String getSourceIP() {
		return sourceIP;
	}
	public void setSourceIP(String sourceIP) {
		this.sourceIP = sourceIP;
	}
	public String getDestinationIP() {
		return destinationIP;
	}
	public void setDestinationIP(String destinationIP) {
		this.destinationIP = destinationIP;
	}
	public Long getPacketsCount() {
		return packetsCount;
	}
	public void setPacketsCount(Long packetsCount) {
		this.packetsCount = packetsCount;
	}
	public Long getRecordKey() {
		return recordKey;
	}
	public void setRecordKey(Long recordKey) {
		this.recordKey = recordKey;
	}
	public Long getOctetsCount() {
		return octetsCount;
	}
	public void setOctetsCount(Long octetsCount) {
		this.octetsCount = octetsCount;
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
	public Timestamp getTimestamp() {
		return timestamp;
	}
	public void setTimestamp(Timestamp timestamp) {
		this.timestamp = timestamp;
	}
	
}
