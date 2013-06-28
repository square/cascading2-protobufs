package com.squareup.cascading2.function;

import cascading.flow.FlowProcess;
import cascading.operation.BaseOperation;
import cascading.operation.Function;
import cascading.operation.FunctionCall;
import cascading.tuple.Fields;
import cascading.tuple.Tuple;
import com.google.protobuf.Descriptors;
import com.google.protobuf.Message;
import com.squareup.cascading2.util.Util;
import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.util.ArrayList;
import java.util.List;

public class ExpandProto<T extends Message> extends BaseOperation implements Function {
  protected final String messageClassName;
  private final String[] fieldsToExtract;

  private transient Descriptors.FieldDescriptor[] fieldDescriptorsToExtract;

  protected static <T extends Message> String[] getAllFields(Class<T> messageClass) {
    try {
      Method m = messageClass.getMethod("newBuilder");
      Message.Builder builder = (Message.Builder) m.invoke(new Object[]{});

      List<String> fieldNames = new ArrayList<String>();
      for (Descriptors.FieldDescriptor fieldDesc : builder.getDescriptorForType().getFields()) {
        fieldNames.add(fieldDesc.getName());
      }
      return fieldNames.toArray(new String[fieldNames.size()]);
    } catch (IllegalAccessException e) {
      throw new RuntimeException(e);
    } catch (NoSuchMethodException e) {
      throw new RuntimeException(e);
    } catch (InvocationTargetException e) {
      throw new RuntimeException(e);
    }
  }

  /** Expand the entire struct, using the same field names as they are found in the struct. */
  public ExpandProto(Class<T> messageClass) {
    this(messageClass, getAllFields(messageClass));
  }

  /** Expand the entire struct, using the supplied field names to name the resultant fields. */
  public ExpandProto(Class<T> messageClass, Fields fieldDeclaration) {
    this(messageClass, fieldDeclaration, getAllFields(messageClass));
  }

  /**
   * Expand only the fields listed in fieldsToExtract, using the same fields names as they are found
   * in the struct.
   */
  public ExpandProto(Class<T> messageClass, String... fieldsToExtract) {
    this(messageClass, new Fields(fieldsToExtract), fieldsToExtract);
  }

  /**
   * Expand only the fields listed in fieldsToExtract, naming them with the corresponding field
   * names in fieldDeclaration.
   */
  public ExpandProto(Class<T> messageClass, Fields fieldDeclaration, String... fieldsToExtract) {
    super(1, fieldDeclaration);
    if (fieldDeclaration.size() != fieldsToExtract.length) {
      throw new IllegalArgumentException("Fields "
          + fieldDeclaration
          + " doesn't have enough field names to identify all "
          + fieldsToExtract.length
          + " fields in "
          + messageClass.getName());
    }

    Message.Builder builder = Util.builderFromMessageClass(messageClass.getName());

    List <Descriptors.FieldDescriptor> fieldDescriptors = new ArrayList<Descriptors.FieldDescriptor>();
    for (int i = 0; i < fieldsToExtract.length; i++) {
      if (builder.getDescriptorForType().findFieldByName(fieldsToExtract[i]) == null) {
        throw new IllegalArgumentException("Could not find a field named '"
            + fieldsToExtract[i]
            + "' in message class "
            + messageClass.getName());
      }
    }

    this.fieldsToExtract = fieldsToExtract;
    this.messageClassName = messageClass.getName();
  }

  @Override public void operate(FlowProcess flowProcess, FunctionCall functionCall) {
    T arg = (T) functionCall.getArguments().getObject(0);
    if (!arg.getClass().getName().equals(messageClassName)) {
      throw new IllegalArgumentException("Expected argument of type " + messageClassName + ", found " + arg.getClass().getName());
    }
    Tuple result = new Tuple();

    for (Descriptors.FieldDescriptor fieldDescriptor : getFieldDescriptorsToExtract()) {
      Object fieldValue = null;
      if (fieldDescriptor.isRepeated()) {
        if (arg.getRepeatedFieldCount(fieldDescriptor) > 0) {
          fieldValue = arg.getRepeatedField(fieldDescriptor, 0);
        }
      } else {
        if (arg.hasField(fieldDescriptor)) {
          fieldValue = arg.getField(fieldDescriptor);
        }
      }
      if (fieldValue != null) {
        if (fieldDescriptor.getJavaType() == Descriptors.FieldDescriptor.JavaType.ENUM) {
          Descriptors.EnumValueDescriptor valueDescriptor =
              (Descriptors.EnumValueDescriptor) fieldValue;
          fieldValue = valueDescriptor.getNumber();
        }
        result.add(fieldValue);
      } else {
        result.add(null);
      }
    }
    functionCall.getOutputCollector().add(result);
  }

  protected Descriptors.FieldDescriptor[] getFieldDescriptorsToExtract() {
    if (fieldDescriptorsToExtract == null) {
      Message.Builder builder = Util.builderFromMessageClass(messageClassName);

      List <Descriptors.FieldDescriptor> fieldDescriptors = new ArrayList<Descriptors.FieldDescriptor>();
      for (int i = 0; i < fieldsToExtract.length; i++) {
        fieldDescriptors.add(builder.getDescriptorForType().findFieldByName(fieldsToExtract[i]));
      }

      fieldDescriptorsToExtract = fieldDescriptors.toArray(new Descriptors.FieldDescriptor[fieldDescriptors.size()]);
    }
    return fieldDescriptorsToExtract;
  }
}
