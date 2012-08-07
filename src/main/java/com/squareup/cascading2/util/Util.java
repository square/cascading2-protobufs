package com.squareup.cascading2.util;

import com.google.protobuf.Message;
import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;

/**
 * Created with IntelliJ IDEA. User: duxbury Date: 8/6/12 Time: 4:42 PM To change this template use
 * File | Settings | File Templates.
 */
public final class Util {
  private Util() {}

  public static Message.Builder builderFromMessageClass(String messageClassName) {
    try {
      Class<Message> builderClass = (Class<Message>) Class.forName(messageClassName);
      Method m = builderClass.getMethod("newBuilder");
      return (Message.Builder) m.invoke(new Object[]{});
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
}
