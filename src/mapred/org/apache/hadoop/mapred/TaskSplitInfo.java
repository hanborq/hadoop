package org.apache.hadoop.mapred;

import java.io.ByteArrayInputStream;
import java.io.DataInput;
import java.io.DataInputStream;
import java.io.DataOutput;
import java.io.IOException;

import org.apache.hadoop.conf.Configurable;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.util.ReflectionUtils;

public class TaskSplitInfo implements Writable, Configurable {
	private String inputSplitClass;
	private byte[] inputSplitObject;
	private Configuration conf;

	public TaskSplitInfo(JobConf conf, SplitInfo splitInfo) {
		this.conf = conf;
		this.setInputSplitClass(splitInfo.getInputSplitClass());
		this.setInputSplitObject(splitInfo.getInputSplitObject());
	}

	public TaskSplitInfo(JobConf conf, String inputSplitClass,
			byte[] inputSplitObject) {
		this.conf = conf;
		this.setInputSplitClass(inputSplitClass);
		this.setInputSplitObject(inputSplitObject);
	}

	public TaskSplitInfo() {

	}
	
	public org.apache.hadoop.mapred.InputSplit getOldSplit() throws ClassNotFoundException, IOException {
		org.apache.hadoop.mapred.InputSplit split = (org.apache.hadoop.mapred.InputSplit)ReflectionUtils.newInstance(conf.getClassByName(inputSplitClass), conf);
		ByteArrayInputStream input = new ByteArrayInputStream(inputSplitObject);
		DataInputStream in = new DataInputStream(input);
		split.readFields(in);
		
		return split;
	}
	
	public org.apache.hadoop.mapreduce.InputSplit getNewSplit() throws IOException, ClassNotFoundException {
		org.apache.hadoop.mapreduce.InputSplit split = (org.apache.hadoop.mapreduce.InputSplit)ReflectionUtils.newInstance(conf.getClassByName(inputSplitClass), conf);
		ByteArrayInputStream input = new ByteArrayInputStream(inputSplitObject);
		DataInputStream in = new DataInputStream(input);
		((Writable)split).readFields(in);
		
		return split;
	}

	@Override
	public void readFields(DataInput in) throws IOException {
		setInputSplitClass(in.readUTF());
		
		int len = in.readInt();
		setInputSplitObject(new byte[len]);
		in.readFully(getInputSplitObject());
	}

	@Override
	public void write(DataOutput out) throws IOException {
		out.writeUTF(inputSplitClass);
		out.writeInt(inputSplitObject.length);
		out.write(inputSplitObject);
	}


	@Override
	public Configuration getConf() {
		return conf;
	}

	@Override
	public void setConf(Configuration conf) {
		this.conf = conf;

	}

	public void setInputSplitClass(String inputSplitClass) {
		this.inputSplitClass = inputSplitClass;
	}

	public String getInputSplitClass() {
		return inputSplitClass;
	}

	public void setInputSplitObject(byte[] inputSplitObject) {
		this.inputSplitObject = inputSplitObject;
	}

	public byte[] getInputSplitObject() {
		return inputSplitObject;
	}

}
