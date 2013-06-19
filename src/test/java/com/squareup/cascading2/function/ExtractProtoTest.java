package com.squareup.cascading2.function;

import cascading.flow.hadoop.HadoopFlowProcess;
import cascading.operation.Function;
import cascading.operation.FunctionCall;
import cascading.tuple.Fields;
import cascading.tuple.Tuple;
import cascading.tuple.TupleEntry;
import cascading.tuple.TupleEntryCollector;
import com.squareup.cascading2.generated.Example;
import java.io.IOException;
import java.util.concurrent.atomic.AtomicReference;
import junit.framework.TestCase;

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

  public void testSomePresent() throws Exception {
    assertEquals(new Tuple(null, null, "Bryan", "bryan@"),
        exec(new ExtractProto(Example.Partnership.class, "follower.name", "follower.email", "leader.name", "leader.email"), new Tuple(P2)));
  }

  private static Tuple exec(Function f, final Tuple input) {
    final AtomicReference<Tuple> output = new AtomicReference<Tuple>();

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
