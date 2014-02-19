package com.squareup.cascading2.serialization;

import cascading.tuple.Comparison;
import cascading.tuple.StreamComparator;
import cascading.tuple.hadoop.io.BufferedInputStream;
import com.google.protobuf.CodedInputStream;
import com.google.protobuf.Message;
import com.squareup.cascading2.util.Util;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.util.Comparator;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.io.WritableComparator;
import org.apache.hadoop.io.serializer.Deserializer;
import org.apache.hadoop.io.serializer.Serialization;
import org.apache.hadoop.io.serializer.Serializer;
import sun.reflect.generics.reflectiveObjects.NotImplementedException;

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
      builder = Util.builderFromMessageClass(messageClass.getName());
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

  private static class ProtobufComparator<T extends Message> implements Comparator<T>, StreamComparator<BufferedInputStream> {
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

    @Override
    public int compare(BufferedInputStream lhs, BufferedInputStream rhs) {
      CodedInputStream clhs = CodedInputStream.newInstance(lhs);
      CodedInputStream crhs = CodedInputStream.newInstance(rhs);

      try {
        int lhsLen = clhs.readRawVarint32();
//        byte[] lhsBytes = new byte[lhsLen];
//        lhs.read(lhsBytes, 0, lhsLen);

        int rhsLen = crhs.readRawVarint32();
//        byte[] rhsBytes = new byte[lhsLen];
//        lhs.read(rhsBytes, 0, rhsLen);

//        return WritableComparator.compareBytes(lhsBytes, 0, lhsLen, rhsBytes, 0, rhsLen);
        return WritableComparator.compareBytes(lhs.getBuffer(), lhs.getPosition(), lhsLen, rhs.getBuffer(), rhs.getPosition(), rhsLen);
      } catch (IOException e) {
        throw new RuntimeException(e);
      }
    }
  }
}
