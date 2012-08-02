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
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import org.apache.hadoop.io.BytesWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.OutputCollector;
import org.apache.hadoop.mapred.RecordReader;
import org.apache.hadoop.mapred.SequenceFileOutputFormat;

public class ProtobufScheme extends SequenceFile {

  private transient Message.Builder prototype;
  private final String fieldName;
  private final String messageClassName;

  public ProtobufScheme(String fieldName, Class<? extends Message> prototype) {
    super(new Fields(fieldName));
    this.fieldName = fieldName;
    messageClassName = prototype.getName();
  }

  @Override public void sourcePrepare(FlowProcess<JobConf> flowProcess,
      SourceCall<Object[], RecordReader> sourceCall) {
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
    //Object key = sourceCall.getContext()[0];
    //Object value = sourceCall.getContext()[1];
    BytesWritable value = new BytesWritable();
    boolean result = sourceCall.getInput().next(NullWritable.get(), value);

    if (!result) return false;

    Tuple tuple = sourceCall.getIncomingEntry().getTuple();
    tuple.clear();

    tuple.add(getPrototype().mergeFrom(value.getBytes(), 0, value.getLength()).build());

    return true;
  }

  private Message.Builder getPrototype() {
    if (prototype == null) {
      try {
        Class<Message> messageClass = (Class<Message>) Class.forName(messageClassName);
        Method m = messageClass.getMethod("newBuilder");
        prototype = (Message.Builder) m.invoke(new Object[]{});
      } catch (IllegalAccessException e) {
        throw new RuntimeException(e);
      } catch (ClassNotFoundException e) {
        throw new RuntimeException(e);
      } catch (NoSuchMethodException e) {
        throw new RuntimeException(e);
      } catch (InvocationTargetException e) {
        throw new RuntimeException(e);
      }
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
