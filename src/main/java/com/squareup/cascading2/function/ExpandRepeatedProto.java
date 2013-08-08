package com.squareup.cascading2.function;

import cascading.operation.BaseOperation;
import cascading.operation.Function;
import com.google.protobuf.Message;
import cascading.flow.FlowProcess;
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

public class ExpandRepeatedProto <T extends Message> extends BaseOperation implements Function {
  private final String messageClassName;
  private final String[] fieldsToExtract;

  private transient Descriptors.FieldDescriptor[] fieldDescriptorsToExtract;

  /** Expand the entire struct, using the same field names as they are found in the struct. */
  public ExpandRepeatedProto(Class<T> messageClass) {
    this(messageClass, ExpandProto.getAllFields(messageClass));
  }

  /** Expand the entire struct, using the supplied field names to name the resultant fields. */
  public ExpandRepeatedProto(Class<T> messageClass, Fields fieldDeclaration) {
    this(messageClass, fieldDeclaration, ExpandProto.getAllFields(messageClass));
  }

  /**
   * Expand only the fields listed in fieldsToExtract, using the same fields names as they are found
   * in the struct.
   */
  public ExpandRepeatedProto(Class<T> messageClass, String... fieldsToExtract) {
    this(messageClass, new Fields(fieldsToExtract), fieldsToExtract);
  }

  /**
   * Expand only the fields listed in fieldsToExtract, naming them with the corresponding field
   * names in fieldDeclaration.
   */
  public ExpandRepeatedProto(Class<T> messageClass, Fields fieldDeclaration,
      String... fieldsToExtract) {
    // Set up fields and perform basic checks
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

    for (int i = 0; i < fieldsToExtract.length; i++) {
      Descriptors.FieldDescriptor field = builder.getDescriptorForType().findFieldByName(fieldsToExtract[i]);
      if (field == null) {
        throw new IllegalArgumentException("Could not find a field named '"
            + fieldsToExtract[i]
            + "' in message class "
            + messageClass.getName());
      }
    }

    this.fieldsToExtract = fieldsToExtract;
    this.messageClassName = messageClass.getName();

    // Check repeated fields
    int repeatedCount = 0;

    for (int i = 0; i < fieldsToExtract.length; i++) {
      Descriptors.FieldDescriptor field =
          builder.getDescriptorForType().findFieldByName(fieldsToExtract[i]);

      if (field.isRepeated()) {
        repeatedCount++;
      }
    }

    String className =
        Util.builderFromMessageClass(messageClass.getName()).getDescriptorForType().getName();
    if (repeatedCount == 0) {
      throw new IllegalArgumentException("None of the requested fields in struct " + className +
          " are repeated.");
    } else if (repeatedCount > 1) {
      throw new UnsupportedOperationException("More than one of the requested fields in struct " +
          className + " is repeated.");
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

    // Build the "prototype" tuple: a tuple that has all the fields that will be the same among
    // the list of resultant tuples.
    Tuple prototypeTuple = new Tuple();
    int repeatedFieldIndex = -1;

    fieldDescriptorsToExtract = ExpandProto.getFieldDescriptorsToExtract(fieldDescriptorsToExtract, messageClassName, fieldsToExtract);
    for (int i = 0; i < fieldDescriptorsToExtract.length; i++) {
      Descriptors.FieldDescriptor fieldDescriptor = fieldDescriptorsToExtract[i];

      if (fieldDescriptor.isRepeated()) {
        repeatedFieldIndex = i;
        prototypeTuple.add(null);
        continue;
      } else {

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
          prototypeTuple.add(fieldValue);
        } else {
          prototypeTuple.add(null);
        }
      }
    }

    Descriptors.FieldDescriptor repeatedField = fieldDescriptorsToExtract[repeatedFieldIndex];
    for (Object value : (List<Object>) arg.getField(repeatedField)) {
      Tuple copy = new Tuple(prototypeTuple);
      copy.set(repeatedFieldIndex, value);

      functionCall.getOutputCollector().add(copy);
    }

  }
}
