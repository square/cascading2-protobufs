package com.squareup.cascading2.function;

import cascading.tuple.Fields;
import cascading.tuple.Tuple;
import com.google.protobuf.ByteString;
import com.squareup.cascading_helpers.util.TestHelpers;
import java.util.Arrays;
import java.util.List;
import junit.framework.TestCase;
import org.junit.Test;

public class DecodeByteStringTest extends TestCase {
  @Test
  public void testByteStringToUTF8() throws Exception {
    List<Tuple> results = TestHelpers.exec(
        new DecodeByteString("blah"),
        new Fields("blah"),
        new Tuple(ByteString.copyFromUtf8("1337")),
        new Tuple(ByteString.copyFromUtf8("brian")),
        new Tuple((ByteString)null));

    assertEquals(Arrays.asList(
        new Tuple("1337"),
        new Tuple("brian"),
        new Tuple((Object)null)),
        results);
  }

  @Test
  public void testByteStringToISO88591() throws Exception {
    List<Tuple> results = TestHelpers.exec(
        new DecodeByteString("ISO-8859-1", "blah"),
        new Fields("blah"),
        new Tuple(ByteString.copyFromUtf8("1337")),
        new Tuple(ByteString.copyFromUtf8("brian")),
        new Tuple((ByteString)null));

    assertEquals(Arrays.asList(
        new Tuple("1337"),
        new Tuple("brian"),
        new Tuple((Object)null)),
        results);
  }

  @Test
  public void testBadEncoding() throws Exception {
    try {
      List<Tuple> results = TestHelpers.exec(
          new DecodeByteString("LOL I DONT EXIST", "blah"),
          new Fields("blah"),
          new Tuple(ByteString.copyFromUtf8("1337")),
          new Tuple(ByteString.copyFromUtf8("brian")));
    } catch (RuntimeException e) {
      return;
    }
    fail("did not raise exception");
 }
}
