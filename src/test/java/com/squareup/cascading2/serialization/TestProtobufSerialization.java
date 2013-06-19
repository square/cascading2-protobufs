package com.squareup.cascading2.serialization;

import cascading.flow.hadoop.HadoopFlowConnector;
import cascading.flow.hadoop.HadoopFlowProcess;
import cascading.operation.Insert;
import cascading.pipe.Each;
import cascading.pipe.GroupBy;
import cascading.pipe.Pipe;
import cascading.tap.Tap;
import cascading.tap.hadoop.Hfs;
import cascading.tuple.Fields;
import cascading.tuple.Tuple;
import cascading.tuple.TupleEntry;
import cascading.tuple.TupleEntryCollector;
import cascading.tuple.TupleEntryIterator;
import com.google.protobuf.Message;
import com.squareup.cascading2.generated.Example;
import com.squareup.cascading2.scheme.ProtobufScheme;
import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;
import junit.framework.TestCase;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.serializer.Deserializer;
import org.apache.hadoop.io.serializer.Serializer;
import org.apache.hadoop.mapred.JobConf;
import org.junit.Ignore;

public class TestProtobufSerialization extends TestCase {

  public void testBareRoundtrip() throws Exception {
    ProtobufSerialization serde = new ProtobufSerialization();
    Serializer<Example.Person> ser = serde.getSerializer(Example.Person.class);
    ByteArrayOutputStream byteArrayOutputStream = new ByteArrayOutputStream();
    ser.open(byteArrayOutputStream);
    ser.serialize(Example.Person.newBuilder().setName("bryan").setId(1).build());
    ser.serialize(Example.Person.newBuilder().setName("lucas").setId(2).build());
    ser.close();

    Deserializer<Example.Person> de = serde.getDeserializer(Example.Person.class);
    de.open(new ByteArrayInputStream(byteArrayOutputStream.toByteArray()));
    Example.Person person = de.deserialize(null);
    assertEquals("bryan", person.getName());
    assertEquals(1, person.getId());

    person = de.deserialize(null);
    assertEquals("lucas", person.getName());
    assertEquals(2, person.getId());
  }

  public void testAsGroupByValue() throws Exception {
    FileSystem.get(new Configuration()).delete(new Path("/tmp/input"), true);
    FileSystem.get(new Configuration()).delete(new Path("/tmp/output"), true);

    Tap t = new Hfs(new ProtobufScheme("value", Example.Person.class), "/tmp/input");
    TupleEntryCollector tec = t.openForWrite(new HadoopFlowProcess(new JobConf()));

    HashSet<Tuple> expectedTuples = new HashSet<Tuple>(){{
      add(new Tuple(Example.Person.newBuilder().setName("bryan").setId(1).build()));
      add(new Tuple(Example.Person.newBuilder().setName("lucas").setId(2).build()));
    }};

    for (Tuple tuple : expectedTuples) {
      tec.add(tuple);
    }

    tec.close();

    Pipe inPipe = new Pipe("input");
    Pipe injectedPipe = new Each(inPipe, Fields.NONE, new Insert(new Fields("key"), 7), new Fields("key", "value"));
    Pipe groupByPipe = new GroupBy(injectedPipe, new Fields("key"));

    Hfs sink = new Hfs(new ProtobufScheme("value", Example.Person.class), "/tmp/output");
    Map<Object, Object> properties = new HashMap<Object, Object>(){{
      put("io.serializations", new JobConf().get("io.serializations") + "," + ProtobufSerialization.class.getName());
    }};
    new HadoopFlowConnector(properties).connect(t, sink, groupByPipe).complete();

    TupleEntryIterator tei = sink.openForRead(new HadoopFlowProcess(new JobConf()));
    Set<Tuple> tuples = new HashSet<Tuple>();
    while (tei.hasNext()) {
      tuples.add(tei.next().getTupleCopy());
    }

    assertEquals(expectedTuples, tuples);
  }

  public void testAsGroupByKey() throws Exception {
    FileSystem.get(new Configuration()).delete(new Path("/tmp/input"), true);
    FileSystem.get(new Configuration()).delete(new Path("/tmp/output"), true);

    Tap t = new Hfs(new ProtobufScheme("value", Example.Person.class), "/tmp/input");
    TupleEntryCollector tec = t.openForWrite(new HadoopFlowProcess(new JobConf()));

    HashSet<Tuple> expectedTuples = new HashSet<Tuple>(){{
      add(new Tuple(Example.Person.newBuilder().setName("bryan").setId(1).build()));
      add(new Tuple(Example.Person.newBuilder().setName("lucas").setId(2).build()));
    }};

    for (Tuple tuple : expectedTuples) {
      tec.add(tuple);
    }

    tec.close();

    Pipe inPipe = new Pipe("input");
    Pipe groupByPipe = new GroupBy(inPipe, new Fields("value"));

    Hfs sink = new Hfs(new ProtobufScheme("value", Example.Person.class), "/tmp/output");
    Map<Object, Object> properties = new HashMap<Object, Object>(){{
      put("io.serializations",
          new JobConf().get("io.serializations") + "," + ProtobufSerialization.class.getName());
    }};
    new HadoopFlowConnector(properties).connect(t, sink, groupByPipe).complete();

    TupleEntryIterator tei = sink.openForRead(new HadoopFlowProcess(new JobConf()));
    Set<Tuple> tuples = new HashSet<Tuple>();
    while (tei.hasNext()) {
      tuples.add(tei.next().getTupleCopy());
    }

    assertEquals(expectedTuples, tuples);
  }
}
