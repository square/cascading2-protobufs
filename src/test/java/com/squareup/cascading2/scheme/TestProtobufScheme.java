package com.squareup.cascading2.scheme;

import cascading.flow.hadoop.HadoopFlowProcess;
import cascading.tap.Tap;
import cascading.tap.hadoop.Hfs;
import cascading.tuple.Fields;
import cascading.tuple.Tuple;
import cascading.tuple.TupleEntry;
import cascading.tuple.TupleEntryCollector;
import cascading.tuple.TupleEntryIterator;
import com.squareup.cascading2.generated.Example;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import junit.framework.TestCase;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;

public class TestProtobufScheme extends TestCase {
  public void testRoundtrip() throws Exception {
    FileSystem.get(new Configuration()).delete(new Path("/tmp/input"), true);

    // write some fake data
    List<Tuple> expected = new ArrayList<Tuple>();
    expected.add(fixture("bryan", "bryan.duxbury@mail.com", 1));
    expected.add(fixture("lucas", "lucas@mail.com", 2));
    expected.add(fixture("vida", null, 3));

    Tap inputTap = new Hfs(new ProtobufScheme("value", Example.Person.class), "/tmp/input");
    TupleEntryCollector tec = inputTap.openForWrite(new HadoopFlowProcess(), null);

    for (Tuple t : expected) {
      tec.add(new TupleEntry(new Fields("value"), t));
    }
    tec.close();

    // read results back out
    Tap outputTap = new Hfs(new ProtobufScheme("value", Example.Person.class), "/tmp/input");
    TupleEntryIterator iter = outputTap.openForRead(new HadoopFlowProcess(), null);
    List<Tuple> tuples = new ArrayList<Tuple>();
    while (iter.hasNext()) {
      tuples.add(iter.next().getTupleCopy());
    }

    assertEquals(new HashSet<Tuple>(expected), new HashSet<Tuple>(tuples));
  }

  private Tuple fixture(String name, String email, int id) {
    Example.Person.Builder builder = Example.Person.newBuilder();
    builder.setId(id);
    if (name != null) {
      builder.setName(name);
    }
    if (email != null) {
      builder.setEmail(email);
    }
    return new Tuple(builder.build());
  }
}
