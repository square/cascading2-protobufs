package com.squareup.cascading2.function;

import cascading.tuple.Fields;
import cascading.tuple.Tuple;
import cascading.tuple.TupleEntry;
import com.squareup.cascading2.generated.Example;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import junit.framework.TestCase;

import static com.squareup.cascading2.function.TestExtensionSupport.COFFEE_DRINK;
import static com.squareup.cascading2.function.TestExtensionSupport.DRINK_ORDER_EXTENSIONS;
import static com.squareup.cascading2.function.TestExtensionSupport.TEA_AND_TWO_COFFEES;
import static com.squareup.cascading2.function.TestExtensionSupport.TEA_DRINK;

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
    List<Tuple> results = ExpandProtoTest.operateFunction(silent,
        new TupleEntry(new Fields("value"), new Tuple(Example.Partnership
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
    ExpandRepeatedProto silent = new ExpandRepeatedProto(Example.Partnership.class, "leader", "silent");
    List<Tuple> results = ExpandProtoTest.operateFunction(silent,
        new TupleEntry(new Fields("value"), new Tuple(Example.Partnership
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

  public void testRepeatedExtension() throws Exception {
    ExpandRepeatedProto drink = ExpandRepeatedProto.expandRepeatedProto(Example.DrinkOrder.class,
        DRINK_ORDER_EXTENSIONS);

    List<Tuple> results = ExpandProtoTest.operateFunction(drink,
        new TupleEntry(new Fields("value"), new Tuple(TEA_AND_TWO_COFFEES)));

    List<Tuple> expected = Arrays.asList(
      new Tuple("Nathan", TEA_DRINK),
      new Tuple("Nathan", COFFEE_DRINK),
      new Tuple("Nathan", COFFEE_DRINK)
    );

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
      new ExpandRepeatedProto<Example.Person>(Example.Person.class, new Fields("1", "2"), "id");
      fail("should throw exception with arg length mismatch");
    } catch(IllegalArgumentException e) {
      // ok
    }
  }

  public void testBadArguments() throws Exception {
    try {
      new ExpandRepeatedProto<Example.Person>(Example.Person.class, "leader");
      fail("should throw exception because this field isn't repeated");
    } catch(IllegalArgumentException e) {
      // ok
    }

    try {
      new ExpandRepeatedProto<Example.Market>(Example.Market.class);
      fail("should throw exception because there are multiple repeated fields");
    } catch(UnsupportedOperationException e) {
      // ok
    }  }
}
