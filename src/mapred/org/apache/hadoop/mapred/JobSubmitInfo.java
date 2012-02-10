package org.apache.hadoop.mapred;

import java.io.ByteArrayOutputStream;
import java.io.DataInput;
import java.io.DataOutput;
import java.io.DataOutputStream;
import java.io.IOException;

import org.apache.hadoop.io.Writable;
import org.apache.hadoop.io.WritableFactories;

public class JobSubmitInfo implements Writable {
	private boolean useNewAPI;
	private SplitInfo[] splitInfos;
	private JobConf jobConf;

	public JobSubmitInfo(JobConf conf,
			org.apache.hadoop.mapred.InputSplit splits[]) throws IOException {
		this.jobConf = conf;
		this.useNewAPI = false;

		setSplitInfos(new SplitInfo[splits.length]);
		ByteArrayOutputStream outputStream = new ByteArrayOutputStream();

		for (int i = 0; i < splits.length; ++i) {
			DataOutputStream dataOutputStream = new DataOutputStream(outputStream);
			String className = splits[i].getClass().getName();
			splits[i].write(dataOutputStream);
			dataOutputStream.flush();

			byte[] object = outputStream.toByteArray();
			outputStream.reset();

			getSplitInfos()[i] = new SplitInfo(className, object, splits[i]
					.getLength(), splits[i].getLocations());
		}

	}

	public JobSubmitInfo(JobConf conf,
			org.apache.hadoop.mapreduce.InputSplit splits[]) throws IOException,
			InterruptedException {
		this.jobConf = conf;
		this.useNewAPI = true;

		setSplitInfos(new SplitInfo[splits.length]);
		ByteArrayOutputStream outputStream = new ByteArrayOutputStream();

		for (int i = 0; i < splits.length; ++i) {
			DataOutputStream dataOutputStream = new DataOutputStream(outputStream);
			String className = splits[i].getClass().getName();
			((Writable) splits[i]).write(dataOutputStream);
			dataOutputStream.flush();

			byte[] object = outputStream.toByteArray();
			outputStream.reset();

			getSplitInfos()[i] = new SplitInfo(className, object, splits[i]
					.getLength(), splits[i].getLocations());
		}

	}

	public JobSubmitInfo() {

	}

	public int getSplitNum() {
		return getSplitInfos().length;
	}
	
	public TaskSplitInfo[] getTaskSplitInfos() {
		TaskSplitInfo[] result = new TaskSplitInfo[splitInfos.length];
		
		for(int i = 0; i < result.length; ++i)
			result[i] = new TaskSplitInfo(jobConf, splitInfos[i]);
		
		return result;
	}

	@Override
	public void readFields(DataInput in) throws IOException {
		jobConf = new JobConf(false);
		jobConf.readFields(in);
		useNewAPI = in.readBoolean();
		int splitNum = in.readInt();

		setSplitInfos(new SplitInfo[splitNum]);

		for (int i = 0; i < splitNum; ++i) {
			getSplitInfos()[i] = new SplitInfo();
			getSplitInfos()[i].readFields(in);
		}
	}

	@Override
	public void write(DataOutput out) throws IOException {
		jobConf.write(out);
		out.writeBoolean(useNewAPI);
		out.writeInt(getSplitNum());

		for (SplitInfo info : getSplitInfos())
			info.write(out);
	}

	public void setUseNewAPI(boolean useNewAPI) {
		this.useNewAPI = useNewAPI;
	}

	public boolean isUseNewAPI() {
		return useNewAPI;
	}

	public void setJobConf(JobConf jobConf) {
		this.jobConf = jobConf;
	}

	public JobConf getJobConf() {
		return jobConf;
	}

	public void setSplitInfos(SplitInfo[] splitInfos) {
		this.splitInfos = splitInfos;
	}

	public SplitInfo[] getSplitInfos() {
		return splitInfos;
	}

}
