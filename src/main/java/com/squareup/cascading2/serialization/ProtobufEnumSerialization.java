package com.squareup.cascading2.serialization;

import com.google.protobuf.CodedInputStream;
import com.google.protobuf.CodedOutputStream;
import com.google.protobuf.Descriptors;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import org.apache.hadoop.io.serializer.Deserializer;
import org.apache.hadoop.io.serializer.Serialization;
import org.apache.hadoop.io.serializer.Serializer;

public class ProtobufEnumSerialization implements Serialization<Descriptors.EnumValueDescriptor> {
  @Override public boolean accept(Class<?> aClass) {
    return Descriptors.EnumValueDescriptor.class.isAssignableFrom(aClass);
  }

  @Override public Serializer<Descriptors.EnumValueDescriptor> getSerializer(Class<Descriptors.EnumValueDescriptor> tClass) {
    return new ProtobufEnumSerializer();
  }

  @Override public Deserializer<Descriptors.EnumValueDescriptor> getDeserializer(Class<Descriptors.EnumValueDescriptor> tClass) {
    return new ProtobufEnumDeserializer(tClass);
  }

  private static class ProtobufEnumSerializer implements Serializer<Descriptors.EnumValueDescriptor> {
    private OutputStream outputStream;

    @Override public void open(OutputStream outputStream) throws IOException {
      this.outputStream = outputStream;
    }

    @Override public void serialize(Descriptors.EnumValueDescriptor enumValueDescriptor)
        throws IOException {
      enumValueDescriptor.toProto().writeDelimitedTo(outputStream);
    }

    @Override public void close() throws IOException {
      outputStream.close();
    }
  }

  private class ProtobufEnumDeserializer implements Deserializer<Descriptors.EnumValueDescriptor> {
    private Class<Descriptors.EnumValueDescriptor> tClass;
    private CodedInputStream codedInputStream;
    private InputStream inputStream;

    public ProtobufEnumDeserializer(Class<Descriptors.EnumValueDescriptor> tClass) {
      this.tClass = tClass;

    }

    @Override public void open(InputStream inputStream) throws IOException {
      this.inputStream = inputStream;
      codedInputStream = CodedInputStream.newInstance(this.inputStream);
    }

    @Override public Descriptors.EnumValueDescriptor deserialize(
        Descriptors.EnumValueDescriptor enumValueDescriptor) throws IOException {


      return null;  //To change body of implemented methods use File | Settings | File Templates.
    }

    @Override public void close() throws IOException {
      inputStream.close();
    }
  }
}
