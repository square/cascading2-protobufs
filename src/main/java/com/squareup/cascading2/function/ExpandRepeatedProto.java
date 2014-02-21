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
import java.util.List;

public class ExpandRepeatedProto <T extends Message> extends BaseOperation implements Function {
  private final String messageClassName;
  private final String[] fieldsToExtract;
  private final SelectedFields selectedFields;

  public static <T extends Message> ExpandRepeatedProto<T> expandRepeatedProto(Class<T> messageClass, ExtensionSupport extensionSupport) {
    List<Descriptors.FieldDescriptor> allFields = Util.getFields(messageClass, extensionSupport);
    String[] fieldNames = Util.getFieldNames(allFields);
    return new ExpandRepeatedProto<T>(messageClass, new Fields(fieldNames), extensionSupport, fieldNames);
  }


  /** Expand the entire struct, using the same field names as they are found in the struct. */
  public ExpandRepeatedProto(Class<T> messageClass) {
    this(messageClass, Util.getAllFields(messageClass));
  }

  /** Expand the entire struct, using the supplied field names to name the resultant fields. */
  public ExpandRepeatedProto(Class<T> messageClass, Fields fieldDeclaration) {
    this(messageClass, fieldDeclaration, ExtensionSupport.NONE, Util.getAllFields(messageClass));
  }

  /**
   * Expand only the fields listed in fieldsToExtract, using the same fields names as they are found
   * in the struct.
   */
  public ExpandRepeatedProto(Class<T> messageClass, String... fieldsToExtract) {
    this(messageClass, new Fields(fieldsToExtract), ExtensionSupport.NONE, fieldsToExtract);
  }

  public ExpandRepeatedProto(Class<T> messageClass, Fields fieldDeclaration, String... fieldsToExtract) {
    this(messageClass, fieldDeclaration, ExtensionSupport.NONE, fieldsToExtract);
  }

  /**
   * Expand only the fields listed in fieldsToExtract, naming them with the corresponding field
   * names in fieldDeclaration.
   */
  public ExpandRepeatedProto(Class<T> messageClass, Fields fieldDeclaration,
      ExtensionSupport extensionSupport, String... fieldsToExtract) {
    // Set up fields and perform basic checks
    super(1, fieldDeclaration);
    this.selectedFields = new SelectedFields(messageClass.getName(), extensionSupport, fieldsToExtract);

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
      Descriptors.FieldDescriptor field = selectedFields.findByName(fieldsToExtract[i]);
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
      Descriptors.FieldDescriptor field = selectedFields.findByName(fieldsToExtract[i]);

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

    List<Descriptors.FieldDescriptor> fields = selectedFields.get();
    for (int i = 0; i < fields.size(); i++) {
      Descriptors.FieldDescriptor fieldDescriptor = fields.get(i);

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

    Descriptors.FieldDescriptor repeatedField = fields.get(repeatedFieldIndex);
    List<Object> repeatedValues = (List<Object>) arg.getField(repeatedField);

    if (repeatedValues.size() == 0) {
      // If there are no copies of the repeated field, return at least one copy of the tuple.
      functionCall.getOutputCollector().add(prototypeTuple);
    } else {
      for (Object value : repeatedValues) {
        Tuple copy = new Tuple(prototypeTuple);
        copy.set(repeatedFieldIndex, value);

        functionCall.getOutputCollector().add(copy);
      }
    }
  }
}
