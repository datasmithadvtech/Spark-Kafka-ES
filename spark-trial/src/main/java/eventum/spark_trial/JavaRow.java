package eventum.spark_trial;

import java.io.Serializable;

/** Java Bean class for converting RDD to DataFrame */
public class JavaRow implements Serializable{
	private Long recordKey;
	private String word;
	public Long getRecordKey() {
		return recordKey;
	}
	public void setRecordKey(Long recordKey) {
		this.recordKey = recordKey;
	}
	public String getWord() {
		return word;
	}
	public void setWord(String word) {
		this.word = word;
	}

}
