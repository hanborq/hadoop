package org.apache.hadoop.mapred;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import org.apache.hadoop.io.Writable;

public class SplitInfo implements Writable {
	private String inputSplitClass;
	private byte[] inputSplitObject;
	private long inputDataLength;
	private String[] locations;
	
	public SplitInfo(String inputSplitClass, byte[] inputSplitObject, long inputDataLength, String[] locations) {
		this.setInputSplitClass(inputSplitClass);
		this.setInputSplitObject(inputSplitObject);
		this.inputDataLength = inputDataLength;
		this.locations = locations;
	}
	
	SplitInfo() {
		
	}

	@Override
	public void readFields(DataInput in) throws IOException {
		setInputSplitClass(in.readUTF());
		int len = in.readInt();
		
		setInputSplitObject(new byte[len]);
		in.readFully(getInputSplitObject());
		inputDataLength = in.readLong();
		
		int locationNum = in.readInt();
		locations = new String[locationNum];
		
		for(int i = 0; i < locationNum; ++i)
			locations[i] = in.readUTF();
	}

	@Override
	public void write(DataOutput out) throws IOException {
		out.writeUTF(getInputSplitClass());
		out.writeInt(getInputSplitObject().length);
		out.write(getInputSplitObject());
		out.writeLong(inputDataLength);
		out.writeInt(locations.length);
		
		for(String location : locations)
			out.writeUTF(location);
	}

	public void setInputSplitObject(byte[] inputSplitObject) {
		this.inputSplitObject = inputSplitObject;
	}

	public byte[] getInputSplitObject() {
		return inputSplitObject;
	}

	public void setInputSplitClass(String inputSplitClass) {
		this.inputSplitClass = inputSplitClass;
	}

	public String getInputSplitClass() {
		return inputSplitClass;
	}

	public void setInputDataLength(long inputDataLength) {
		this.inputDataLength = inputDataLength;
	}

	public long getInputDataLength() {
		return inputDataLength;
	}

	public void setLocations(String[] locations) {
		this.locations = locations;
	}

	public String[] getLocations() {
		return locations;
	}

}
