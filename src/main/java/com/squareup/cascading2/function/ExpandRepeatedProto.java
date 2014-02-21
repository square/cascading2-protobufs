package com.squareup.cascading2.function;

import cascading.operation.BaseOperation;
import cascading.operation.Function;
import com.google.protobuf.Message;
import cascading.flow.FlowProcess;
import cascading.operation.FunctionCall;
import cascading.tuple.Fields;
import cascading.tuple.Tuple;
import com.google.protobuf.Descriptors;
import com.squareup.cascading2.util.Util;
import com.squareup.cascading_helpers.operation.KnowsEmittedClasses;

import java.util.HashSet;
import java.util.List;
import java.util.Set;

public class ExpandRepeatedProto<T extends Message> extends AbstractExpandProto<T> {
  /**
   * Expand the specified repeated field, keeping the same field name for the Tuple.
   */
  public ExpandRepeatedProto(Class<T> messageClass, String fieldName) {
    // Set up fields and perform basic checks
    super(messageClass, new Fields(fieldName), new String[]{fieldName});

    Message.Builder builder = Util.builderFromMessageClass(messageClass.getName());

    Descriptors.FieldDescriptor field = builder.getDescriptorForType().findFieldByName(fieldName);
    if (field == null) {
      throw new IllegalArgumentException("No field named '"
          + fieldName
          + "' in message class "
          + messageClass.getName());
    }

    if (!field.isRepeated()) {
      throw new IllegalArgumentException("Field " + fieldName + " is not a repeated field in message class " + messageClass.getName() + ".");
    }
  }

  /**
   * Builds a list of tuples taking into account repeated fields.
   * Only one of the fields is allowed to be repeated.
   * The resultant list will have one tuple per entry in the repeated list;
   * all other fields in the tuples will be identical.
   *
   * @param flowProcess
   * @param functionCall
   */
  @Override public void operate(FlowProcess flowProcess, FunctionCall functionCall) {
    T arg = (T) functionCall.getArguments().getObject(0);
    if (!arg.getClass().getName().equals(messageClassName)) {
      throw new IllegalArgumentException("Expected argument of type " + messageClassName + ", found " + arg.getClass().getName());
    }

    Descriptors.FieldDescriptor fieldDescriptor = getFieldDescriptorsToExtract()[0];

    List<Object> repeatedValues = (List<Object>) arg.getField(fieldDescriptor);

    for (Object o : repeatedValues) {
      functionCall.getOutputCollector().add(new Tuple(o));
    }
  }
}
