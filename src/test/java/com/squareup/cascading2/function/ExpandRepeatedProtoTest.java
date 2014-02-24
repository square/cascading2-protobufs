package com.squareup.cascading2.function;

import cascading.tuple.Fields;
import cascading.tuple.Tuple;
import cascading.tuple.TupleEntry;
import com.squareup.cascading2.generated.Example;
import junit.framework.TestCase;

import java.util.ArrayList;
import java.util.List;

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
  private static final Example.Person LUCAS =
      Example.Person.newBuilder().setName("lucas").setId(2).build();
  private static final Example.Person TOM =
      Example.Person.newBuilder().setName("tom").setId(3).build();
  private static final Example.Person DICK =
      Example.Person.newBuilder().setName("dick").setId(3).build();
  private static final Example.Person HARRY =
      Example.Person.newBuilder().setName("harry").setId(3).build();


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
      expected.add(new Tuple(TOM));
      expected.add(new Tuple(DICK));
      expected.add(new Tuple(HARRY));

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
