package com.squareup.cascading2.serialization;

import cascading.tuple.Comparison;
import com.google.protobuf.Message;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.util.Comparator;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.io.WritableComparator;
import org.apache.hadoop.io.serializer.Deserializer;
import org.apache.hadoop.io.serializer.Serialization;
import org.apache.hadoop.io.serializer.Serializer;

/**
 * Created with IntelliJ IDEA. User: duxbury Date: 8/3/12 Time: 10:12 AM To change this template use
 * File | Settings | File Templates.
 */
public class ProtobufSerialization<T extends Message> extends Configured implements Serialization<T>,
    Comparison<T> {
  @Override public boolean accept(Class<?> aClass) {
    return Message.class.isAssignableFrom(aClass);
  }

  @Override public Serializer<T> getSerializer(Class<T> messageClass) {
    return new ProtobufSerializer();
  }

  @Override public Deserializer<T> getDeserializer(Class<T> messageClass) {
    return new ProtobufDeserializer(messageClass);
  }

  @Override public Comparator<T> getComparator(Class<T> messageClass) {
    return new ProtobufComparator();
  }

  private static class ProtobufSerializer<T extends Message> implements Serializer<T> {
    private OutputStream outputStream;

    @Override public void open(OutputStream outputStream) throws IOException {
      this.outputStream = outputStream;
    }

    @Override public void serialize(T message) throws IOException {
      message.writeDelimitedTo(outputStream);
      outputStream.flush();
    }

    @Override public void close() throws IOException {
      outputStream.close();
    }
  }

  private static class ProtobufDeserializer<T extends Message> implements Deserializer<T> {
    private InputStream inputStream;
    private final Message.Builder builder;

    public ProtobufDeserializer(Class<T> messageClass) {
      try {
        Method m = messageClass.getMethod("newBuilder");
        builder = (Message.Builder) m.invoke(new Object[]{});
      } catch (IllegalAccessException e) {
        throw new RuntimeException(e);
      } catch (NoSuchMethodException e) {
        throw new RuntimeException(e);
      } catch (InvocationTargetException e) {
        throw new RuntimeException(e);
      }
    }

    @Override public void open(InputStream inputStream) throws IOException {
      this.inputStream = inputStream;
    }

    @Override public T deserialize(T message) throws IOException {
      builder.clear();
      builder.mergeDelimitedFrom(inputStream);
      return (T)builder.build();
    }

    @Override public void close() throws IOException {
      inputStream.close();
    }
  }

  private static class ProtobufComparator<T extends Message> implements Comparator<T> {
    @Override public int compare(T message, T message1) {
      try {
        ByteArrayOutputStream baos1 = new ByteArrayOutputStream();
        message.writeTo(baos1);

        ByteArrayOutputStream baos2 = new ByteArrayOutputStream();
        message1.writeTo(baos2);

        byte[] b1 = baos1.toByteArray();
        byte[] b2 = baos2.toByteArray();
        return WritableComparator.compareBytes(b1, 0, b1.length, b2, 0, b2.length);
      } catch (IOException e) {
        throw new RuntimeException(e);
      }
    }
  }
}
