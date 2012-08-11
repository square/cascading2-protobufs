package com.squareup.cascading2.scheme;

import cascading.flow.FlowProcess;
import cascading.scheme.SinkCall;
import cascading.scheme.SourceCall;
import cascading.scheme.hadoop.SequenceFile;
import cascading.tap.Tap;
import cascading.tuple.Fields;
import cascading.tuple.Tuple;
import cascading.tuple.TupleEntry;
import com.google.protobuf.Message;
import com.squareup.cascading2.util.Util;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import org.apache.hadoop.io.BytesWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.OutputCollector;
import org.apache.hadoop.mapred.RecordReader;
import org.apache.hadoop.mapred.SequenceFileOutputFormat;

/**
 * A Scheme that allows reading from and writing to Hadoop SequenceFiles that use NullWritable keys
 * and Protocol Buffers serialized objects wrapped in BytesWritable values.
 */
public class ProtobufScheme extends SequenceFile {
	private transient Message.Builder prototype;
	private final String fieldName;
	private final String messageClassName;

	public ProtobufScheme(String fieldName, Class<? extends Message> messageClass) {
		super(new Fields(fieldName));
		this.fieldName = fieldName;
		messageClassName = messageClass.getName();
	}

	@Override public void sourcePrepare(FlowProcess<JobConf> flowProcess,
			SourceCall<Object[], RecordReader> sourceCall) {
		Object[] pair = new Object[]{sourceCall.getInput().createKey(), 
				sourceCall.getInput().createValue()};
		sourceCall.setContext( pair );
	}

	@Override
	public void sourceCleanup( FlowProcess<JobConf> flowProcess, SourceCall<Object[], RecordReader> sourceCall )
	{
		sourceCall.setContext( null );
	}

	@Override
	public void sinkConfInit(FlowProcess<JobConf> flowProcess,
			Tap<JobConf, RecordReader, OutputCollector> tap, JobConf conf) {
		conf.setOutputKeyClass(NullWritable.class);
		conf.setOutputValueClass(BytesWritable.class);

		conf.setOutputFormat(SequenceFileOutputFormat.class);
	}

	@Override
	public boolean source(FlowProcess<JobConf> flowProcess,
			SourceCall<Object[], RecordReader> sourceCall) throws IOException {
		// TODO: cache this BytesWritable in the context
		Object key = sourceCall.getContext()[ 0 ];
		Object val = sourceCall.getContext()[ 1 ];

		boolean result = sourceCall.getInput().next(key, val);
		if (!result) return false;

		Tuple tuple = sourceCall.getIncomingEntry().getTuple();
		tuple.clear();

		BytesWritable value = (BytesWritable)val;
		tuple.add(getPrototype().mergeFrom(value.getBytes(), 0, value.getLength()).build());

		return true;
	}

	private Message.Builder getPrototype() {
		if (prototype == null) {
			prototype = Util.builderFromMessageClass(messageClassName);
		}
		return prototype;
	}

	@Override
	public void sink(FlowProcess<JobConf> flowProcess, SinkCall<Void, OutputCollector> sinkCall)
			throws IOException {
		TupleEntry tupleEntry = sinkCall.getOutgoingEntry();

		Message message = (Message)tupleEntry.getObject(fieldName);
		ByteArrayOutputStream baos = new ByteArrayOutputStream();
		message.writeTo(baos);
		// TODO: cache this BytesWritable
		BytesWritable outputWritable = new BytesWritable(baos.toByteArray());

		sinkCall.getOutput().collect(NullWritable.get(), outputWritable);
	}

	@Override
	public boolean equals(Object object) {
		if (this == object) return true;
		if (!(object instanceof ProtobufScheme)) return false;
		if (!super.equals(object)) return false;

		// TODO: reimplement this

		return true;
	}
}
