package com.squareup.cascading2.function;

import cascading.flow.hadoop.HadoopFlowConnector;
import cascading.flow.hadoop.HadoopFlowProcess;
import cascading.operation.Function;
import cascading.operation.FunctionCall;
import cascading.pipe.Each;
import cascading.pipe.Pipe;
import cascading.scheme.hadoop.TextLine;
import cascading.tap.hadoop.Hfs;
import cascading.tuple.Fields;
import cascading.tuple.Tuple;
import cascading.tuple.TupleEntry;
import cascading.tuple.TupleEntryCollector;
import cascading.tuple.TupleEntryIterator;
import com.squareup.cascading2.generated.Example;
import com.squareup.cascading2.scheme.ProtobufScheme;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import junit.framework.TestCase;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;

public class ExpandRepeatedProtoTest extends TestCase {
    private static final Example.Partnership P1 = Example.Partnership.newBuilder()
      .setLeader(Example.Person.newBuilder().setName("John").setEmail("john@"))
      .setFollower(Example.Person.newBuilder().setName("Paul").setEmail("paul@"))
      .addSilent(Example.Person.newBuilder().setName("George").setEmail("george@"))
      .addSilent(Example.Person.newBuilder().setName("Ringo").setEmail("ringo@"))
      .build();

  private static final Example.Person.Builder BRYAN = Example.Person
      .newBuilder()
      .setName("bryan")
      .setId(1)
      .setEmail("bryan@mail.com")
      .setPosition(Example.Person.Position.CEO);
  private static final Example.Person.Builder LUCAS =
      Example.Person.newBuilder().setName("lucas").setId(2);
  private static final Example.Person.Builder TOM =
      Example.Person.newBuilder().setName("tom").setId(3);
  private static final Example.Person.Builder DICK =
      Example.Person.newBuilder().setName("dick").setId(3);
  private static final Example.Person.Builder HARRY =
      Example.Person.newBuilder().setName("harry").setId(3);


  public void testOnlyRepeated() throws Exception {
    ExpandRepeatedProto silent = new ExpandRepeatedProto(Example.Partnership.class, "silent");
    List<Tuple> results = TestExpandProto.operateFunction(silent, new TupleEntry(new Fields("value"), new Tuple(
        Example.Partnership
            .newBuilder()
            .setLeader(BRYAN)
            .setFollower(LUCAS)
            .addSilent(TOM)
            .addSilent(DICK)
            .addSilent(HARRY)
            .build())));

      List<Tuple> expected = new ArrayList<Tuple>();
      expected.add(new Tuple(TOM.build()));
      expected.add(new Tuple(DICK.build()));
      expected.add(new Tuple(HARRY.build()));

      assertEquals(expected, results);
  }

  public void testRepeated() throws Exception {
    ExpandRepeatedProto silent = new ExpandRepeatedProto(Example.Partnership.class, "silent");
    List<Tuple> results = TestExpandProto.operateFunction(silent, new TupleEntry(new Fields("value"), new Tuple(
        Example.Partnership
            .newBuilder()
            .setLeader(BRYAN)
            .setFollower(LUCAS)
            .addSilent(TOM)
            .addSilent(DICK)
            .addSilent(HARRY)
            .build())));

    List<Tuple> expected = new ArrayList<Tuple>();
    expected.add(new Tuple(BRYAN.build(), TOM.build()));
    expected.add(new Tuple(BRYAN.build(), DICK.build()));
    expected.add(new Tuple(BRYAN.build(), HARRY.build()));

    assertEquals(expected, results);
  }

   public void testConstructorErrorCases() throws Exception {
    try {
      new ExpandRepeatedProto<Example.Person>(Example.Person.class, "1");
      fail("should throw exception with non-found field");
    } catch(IllegalArgumentException e) {
      // ok
    }

    try {
      new ExpandRepeatedProto<Example.Partnership>(Example.Partnership.class, "leader");
      fail("should throw exception with non-repeated field");
    } catch(IllegalArgumentException e) {
      // ok
    }
  }
}
