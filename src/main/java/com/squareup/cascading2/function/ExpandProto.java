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
import com.squareup.cascading_helpers.operation.KnowsEmittedClasses;

import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

public class ExpandProto<T extends Message> extends AbstractExpandProto<T> {

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
    super(messageClass, fieldDeclaration, fieldsToExtract);
    if (fieldDeclaration.size() != fieldsToExtract.length) {
      throw new IllegalArgumentException("Fields "
          + fieldDeclaration
          + " doesn't have enough field names to identify all "
          + fieldsToExtract.length
          + " fields in "
          + messageClass.getName());
    }

    Message.Builder builder = Util.builderFromMessageClass(messageClass.getName());

    for (int i = 0; i < fieldsToExtract.length; i++) {
      Descriptors.FieldDescriptor field = builder.getDescriptorForType().findFieldByName(fieldsToExtract[i]);
      if (field == null) {
        throw new IllegalArgumentException("Could not find a field named '"
            + fieldsToExtract[i]
            + "' in message class "
            + messageClass.getName());
      } else if (field.isRepeated()) {
        throw new IllegalArgumentException("field "  + fieldsToExtract[i]
            + " is repeated. Please use ExpandRepeatedProto instead.");
      }
    }

  }

  @Override public void operate(FlowProcess flowProcess, FunctionCall functionCall) {
    T arg = (T) functionCall.getArguments().getObject(0);
    if (!arg.getClass().getName().equals(messageClassName)) {
      throw new IllegalArgumentException("Expected argument of type " + messageClassName + ", found " + arg.getClass().getName());
    }
    Tuple result = new Tuple();

    for (Descriptors.FieldDescriptor fieldDescriptor : getFieldDescriptorsToExtract()) {
      Object fieldValue = null;
      if (arg.hasField(fieldDescriptor)) {
        fieldValue = arg.getField(fieldDescriptor);
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
}
