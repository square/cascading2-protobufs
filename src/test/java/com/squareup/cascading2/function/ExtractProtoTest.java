package com.squareup.cascading2.function;

import cascading.flow.FlowDef;
import cascading.flow.hadoop.HadoopFlowConnector;
import cascading.flow.hadoop.HadoopFlowProcess;
import cascading.operation.Function;
import cascading.operation.FunctionCall;
import cascading.pipe.Each;
import cascading.pipe.Pipe;
import cascading.tap.hadoop.Hfs;
import cascading.tuple.Fields;
import cascading.tuple.Tuple;
import cascading.tuple.TupleEntry;
import cascading.tuple.TupleEntryCollector;
import com.squareup.cascading2.generated.Example;
import com.squareup.cascading2.scheme.ProtobufScheme;
import java.io.IOException;
import java.util.concurrent.atomic.AtomicReference;
import junit.framework.TestCase;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;

import static com.squareup.cascading2.function.TestExtensionSupport.DRINK_EXTENSIONS;
import static com.squareup.cascading2.function.TestExtensionSupport.TEA_DRINK;

public class ExtractProtoTest extends TestCase {
  private static final Example.Partnership P1 = Example.Partnership.newBuilder()
      .setFollower(Example.Person.newBuilder()
          .setName("Andy")
          .setEmail("andy@")
      )
      .setLeader(Example.Person.newBuilder()
          .setName("Bryan")
          .setEmail("bryan@")
      )
      .build();

  private static final Example.Partnership P2 = Example.Partnership.newBuilder()
      .setLeader(Example.Person.newBuilder().setName("Bryan").setEmail("bryan@"))
      .build();

  private static final Example.Partnership P3 = Example.Partnership.newBuilder()
      .setLeader(Example.Person.newBuilder()
          .setName("Jack")
          .setEmail("jack@")
          .setPosition(Example.Person.Position.CEO))
      .build();

  public void testValidatesNonExistingField() throws Exception {
    try {
      new ExtractProto(Example.Partnership.class, "follower.has_mind_powers");
      fail("Expected an exception!");
    } catch (IllegalArgumentException e) {
      // yay!
    }
  }

  public void testThrowsOnRepeatedField() throws Exception {
    try {
      new ExtractProto(Example.Partnership.class, "silent.name");
      fail("Expected an exception!");
    } catch (IllegalArgumentException e) {
      // yay!
    }
    try {
      new ExtractProto(Example.Partnership.class, "silent");
      fail("Expected an exception!");
    } catch (IllegalArgumentException e) {
      // yay!
    }
  }

  public void testNullFirstObject() throws Exception {
    assertEquals(new Tuple(null, null, null, null),
        exec(new ExtractProto(Example.Partnership.class, "follower.name", "follower.email", "leader.name", "leader.email"), new Tuple((Object)null)));
  }

  public void testAllPresent() throws Exception {
    assertEquals(new Tuple("Andy", "andy@", "Bryan", "bryan@"),
        exec(new ExtractProto(Example.Partnership.class, "follower.name", "follower.email", "leader.name", "leader.email"), new Tuple(P1)));
  }

  public void testExtension() throws Exception {
    assertEquals(new Tuple(true), exec(new ExtractProto(Example.Drink.class, DRINK_EXTENSIONS, "tea.milk"), new Tuple(TEA_DRINK)));
  }

  public void testSomePresent() throws Exception {
    assertEquals(new Tuple(null, null, "Bryan", "bryan@"),
        exec(new ExtractProto(Example.Partnership.class, "follower.name", "follower.email", "leader.name", "leader.email"), new Tuple(P2)));
  }

  public void testMissingLeaves() throws Exception {
    assertEquals(new Tuple(null, null),
        exec(new ExtractProto(Example.Partnership.class, "leader.id", "leader.position"), new Tuple(P2)));
  }

  public void testEnum() throws Exception {
    assertEquals(new Tuple("Jack", "jack@", Example.Person.Position.CEO.getNumber()),
        exec(new ExtractProto(Example.Partnership.class, "leader.name", "leader.email", "leader.position"), new Tuple(P3)));
  }

  public void testSerialization() throws Exception {
    Pipe p = new Each("input", new ExtractProto(Example.Partnership.class, "leader.name"));

    final String INPUT_PATH = "/tmp/ExtractProtoTest/input";
    final String OUTPUT_PATH = "/tmp/ExtractProtoTest/output";

    Hfs inputTap = new Hfs(new ProtobufScheme("partnership", Example.Partnership.class),
        INPUT_PATH);
    Hfs outputTap = new Hfs(new ProtobufScheme("leader.name", Example.Partnership.class),
        OUTPUT_PATH);

    // make sure the input path exists
    inputTap.openForWrite(new HadoopFlowProcess()).close();
    // make sure the output path does not exist
    FileSystem.get(new Configuration()).delete(new Path(OUTPUT_PATH), true);

    FlowDef flowDef = new FlowDef()
        .addSource("input", inputTap)
        .addSink(p, outputTap)
        .addTail(p);

    new HadoopFlowConnector().connect(flowDef).complete();
  }

  private static Tuple exec(Function f, final Tuple input) {
    final AtomicReference<Tuple> output = new AtomicReference<Tuple>();
    f.prepare(new HadoopFlowProcess(), null);

    f.operate(new HadoopFlowProcess(), new FunctionCall() {
      @Override public TupleEntry getArguments() {
        return new TupleEntry(new Fields("blah"), input);
      }

      @Override public Fields getDeclaredFields() {
        return null;
      }

      @Override public TupleEntryCollector getOutputCollector() {
        return new TupleEntryCollector() {
          @Override protected void collect(TupleEntry tupleEntry) throws IOException {
            output.set(tupleEntry.getTuple());
          }
        };
      }

      @Override public Object getContext() {
        return null;
      }

      @Override public void setContext(Object o) {
      }

      @Override public Fields getArgumentFields() {
        return null;
      }
    });
    return output.get();
  }
}
