package com.squareup.cascading2.function;

import cascading.operation.BaseOperation;
import cascading.operation.Function;
import cascading.tuple.Fields;
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

public abstract class AbstractExpandProto<T extends Message> extends BaseOperation implements Function, KnowsEmittedClasses {
  protected final String messageClassName;
  protected final String[] fieldsToExtract;
  protected transient Descriptors.FieldDescriptor[] fieldDescriptorsToExtract;

  protected AbstractExpandProto(Class<T> messageClass, Fields fieldDeclaration, String[] fieldsToExtract) {
    super(1, fieldDeclaration);
    this.messageClassName = messageClass.getName();
    this.fieldsToExtract = fieldsToExtract;
  }

  protected static <T extends Message> String[] getAllFields(Class<T> messageClass) {
    Message.Builder builder = Util.builderFromMessageClass(messageClass.getName());

    List<String> fieldNames = new ArrayList<String>();
    for (Descriptors.FieldDescriptor fieldDesc : builder.getDescriptorForType().getFields()) {
      fieldNames.add(fieldDesc.getName());
    }
    return fieldNames.toArray(new String[fieldNames.size()]);
  }

  protected Descriptors.FieldDescriptor[] getFieldDescriptorsToExtract() {
    if (fieldDescriptorsToExtract == null) {
      Message.Builder builder = Util.builderFromMessageClass(messageClassName);

      List <Descriptors.FieldDescriptor> fieldDescriptors = new ArrayList<Descriptors.FieldDescriptor>();
      for (String fieldName : fieldsToExtract) {
        fieldDescriptors.add(builder.getDescriptorForType().findFieldByName(fieldName));
      }

      fieldDescriptorsToExtract = fieldDescriptors.toArray(new Descriptors.FieldDescriptor[fieldDescriptors.size()]);
    }

    return fieldDescriptorsToExtract;
  }

  @Override
  public final Set<Class> getEmittedClasses() {
    Set<Class> results = new HashSet<Class>();
    for (Descriptors.FieldDescriptor fieldDesc : getFieldDescriptorsToExtract()) {
      Descriptors.FieldDescriptor.Type type = fieldDesc.getType();
      if (type.getJavaType() == Descriptors.FieldDescriptor.JavaType.MESSAGE) {
        results.add(Util.messageClassFromFieldDesc(messageClassName, fieldDesc));
      }
    }

    return results;
  }
}
