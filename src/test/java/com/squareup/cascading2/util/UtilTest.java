package com.squareup.cascading2.util;

import com.google.protobuf.Descriptors;
import com.squareup.cascading2.generated.Example;
import junit.framework.TestCase;

public class UtilTest extends TestCase {
  public void testMessageClassFromFieldDesc() throws Exception {
    Descriptors.FieldDescriptor desc = Example.Partnership.getDescriptor().findFieldByName("leader");
    assertEquals(Example.Person.class, Util.messageClassFromFieldDesc(Example.Partnership.class.getName(), desc));
  }
}
