package com.squareup.cascading2.serialization;

import com.google.protobuf.ProtocolMessageEnum;
import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import org.apache.hadoop.io.serializer.Deserializer;
import org.apache.hadoop.io.serializer.Serialization;
import org.apache.hadoop.io.serializer.Serializer;

public class ProtobufEnumValueSerialization implements Serialization<ProtocolMessageEnum> {
  @Override public boolean accept(Class<?> aClass) {
    return ProtocolMessageEnum.class.isAssignableFrom(aClass);
  }

  @Override public Serializer<ProtocolMessageEnum> getSerializer(Class<ProtocolMessageEnum> tClass) {
    return new ProtobufEnumValueSerializer();
  }

  @Override public Deserializer<ProtocolMessageEnum> getDeserializer(Class<ProtocolMessageEnum> tClass) {
    return new ProtobufEnumValueDeserializer(tClass);
  }

  private static class ProtobufEnumValueSerializer implements Serializer<ProtocolMessageEnum> {
    private DataOutputStream dataOutputStream;

    @Override public void open(OutputStream outputStream) throws IOException {
      this.dataOutputStream = new DataOutputStream(outputStream);
    }

    @Override public void serialize(ProtocolMessageEnum enumValue) throws IOException {
      dataOutputStream.writeInt(enumValue.getNumber());
    }

    @Override public void close() throws IOException {
      dataOutputStream.close();
    }
  }

  private class ProtobufEnumValueDeserializer implements Deserializer<ProtocolMessageEnum> {
    private Method valueOf;
    private DataInputStream dataInputStream;

    public ProtobufEnumValueDeserializer(Class<ProtocolMessageEnum> tClass) {
      try {
        valueOf = tClass.getMethod("valueOf", Integer.TYPE);
      } catch (NoSuchMethodException e) {
        throw new RuntimeException(e);
      }
    }

    @Override public void open(InputStream inputStream) throws IOException {
      this.dataInputStream = new DataInputStream(inputStream);
    }

    @Override public ProtocolMessageEnum deserialize(ProtocolMessageEnum enumValue) throws IOException {
      try {
        return (ProtocolMessageEnum)valueOf.invoke(null, this.dataInputStream.readInt());
      } catch (IllegalAccessException e) {
        throw new RuntimeException(e);
      } catch (InvocationTargetException e) {
        throw new RuntimeException(e);
      }
    }

    @Override public void close() throws IOException {
      dataInputStream.close();
    }
  }
}
