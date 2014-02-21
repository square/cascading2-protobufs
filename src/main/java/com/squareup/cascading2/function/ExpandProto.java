package com.squareup.cascading2.function;

import cascading.flow.FlowProcess;
import cascading.operation.BaseOperation;
import cascading.operation.Function;
import cascading.operation.FunctionCall;
import cascading.tuple.Fields;
import cascading.tuple.Tuple;
import com.google.protobuf.Descriptors;
import com.google.protobuf.Message;
import com.squareup.cascading2.util.ExtensionSupport;
import com.squareup.cascading2.util.SelectedFields;
import com.squareup.cascading2.util.Util;
import java.util.ArrayList;
import java.util.List;

public class ExpandProto<T extends Message> extends BaseOperation implements Function {
  private final String messageClassName;
  protected final SelectedFields selectedFields;

  public static <T extends Message> ExpandProto<T> expandProto(Class<T> messageClass,
      ExtensionSupport extensionSupport) {
    List<Descriptors.FieldDescriptor> allFields =
        Util.getFields(messageClass, extensionSupport);
    String[] fieldNames = Util.getFieldNames(allFields);
    return new ExpandProto<T>(messageClass, new Fields(fieldNames), extensionSupport, fieldNames);
  }

  /** Expand the entire struct, using the same field names as they are found in the struct. */
  public ExpandProto(Class<T> messageClass) {
    this(messageClass, Util.getAllFields(messageClass));
  }


  /** Expand the entire struct, using the supplied field names to name the resultant fields. */
  public ExpandProto(Class<T> messageClass, Fields fieldDeclaration) {
    this(messageClass, fieldDeclaration, Util.getAllFields(messageClass));
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
    this(messageClass, fieldDeclaration, ExtensionSupport.NONE, fieldsToExtract);
  }

  /**
   * Expand only the fields listed in fieldsToExtract, naming them with the corresponding field
   * names in fieldDeclaration.
   */
  public ExpandProto(Class<T> messageClass, Fields fieldDeclaration, ExtensionSupport extensionSupport, String... fieldNamesToExtract) {
    super(1, fieldDeclaration);
    this.messageClassName = messageClass.getName();
    this.selectedFields = new SelectedFields(messageClass.getName(), extensionSupport, fieldNamesToExtract);

    if (fieldDeclaration.size() != fieldNamesToExtract.length) {
      throw new IllegalArgumentException("Fields "
          + fieldDeclaration
          + " doesn't have enough field names to identify all "
          + fieldNamesToExtract.length
          + " fields in "
          + messageClass.getName());
    }

    for (int i = 0; i < fieldNamesToExtract.length; i++) {
      Descriptors.FieldDescriptor field = selectedFields.findByName(fieldNamesToExtract[i]);
      if (field == null) {
        throw new IllegalArgumentException("Could not find a field named '"
            + fieldNamesToExtract[i]
            + "' in message class "
            + messageClass.getName());
      } else if (field.isRepeated()) {
        throw new IllegalArgumentException("field "  + fieldNamesToExtract[i]
            + " is repeated. Please use ExpandRepeatedProto instead.");
      }
    }
  }

  @Override public void operate(FlowProcess flowProcess, FunctionCall functionCall) {

    T arg = (T) functionCall.getArguments().getObject(0);
    if (!arg.getClass().getName().equals(messageClassName)) {
      throw new IllegalArgumentException(
          "Expected argument of type " + messageClassName + ", found " + arg.getClass().getName());
    }
    Tuple result = new Tuple();

    for (Descriptors.FieldDescriptor fieldDescriptor : selectedFields.get()) {
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

  protected static Descriptors.FieldDescriptor[] getFieldDescriptorsToExtract(
      Descriptors.FieldDescriptor[] fieldDescriptorsToExtract,
      String messageClassName,
      String[] fieldsToExtract) {
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
