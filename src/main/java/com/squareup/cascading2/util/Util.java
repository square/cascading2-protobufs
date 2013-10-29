package com.squareup.cascading2.util;

import com.google.protobuf.Descriptors;
import com.google.protobuf.Message;
import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.util.ArrayList;
import java.util.List;

public final class Util {
  private Util() {}

  public static Message.Builder builderFromMessageClass(String messageClassName) {
    try {
      Class<Message> builderClass = (Class<Message>) Class.forName(messageClassName);
      Method m = builderClass.getMethod("newBuilder");
      return (Message.Builder) m.invoke(new Object[]{});
    } catch (IllegalAccessException e) {
      throw new RuntimeException(e);
    } catch (ClassNotFoundException e) {
      throw new RuntimeException(e);
    } catch (NoSuchMethodException e) {
      throw new RuntimeException(e);
    } catch (InvocationTargetException e) {
      throw new RuntimeException(e);
    }
  }

  /** @return the names of all fields on the given message class, with no support for extensions. */
  public static <T extends Message> String[] getAllFields(Class<T> messageClass) {
    return getFieldNames(getFields(messageClass, ExtensionSupport.NONE));
  }

  /** @return names for direct fields, full names for extension fields. */
  public static String[] getFieldNames(List<Descriptors.FieldDescriptor> fields) {
    List<String> fieldNames = new ArrayList<String>();
    for (Descriptors.FieldDescriptor fieldDesc : fields) {
      if (fieldDesc.isExtension()) {
        fieldNames.add(fieldDesc.getFullName());
      } else {
        fieldNames.add(fieldDesc.getName());
      }
    }
    return fieldNames.toArray(new String[fieldNames.size()]);
  }

  /** @return descriptors for all fields on messageClass */
  public static List<Descriptors.FieldDescriptor> getFields(Class messageClass,
      ExtensionSupport extensionSupport) {
    Message.Builder builder;
    try {
      Method m = messageClass.getMethod("newBuilder");
      builder = (Message.Builder) m.invoke(new Object[]{});
    } catch (IllegalAccessException e) {
      throw new RuntimeException(e);
    } catch (NoSuchMethodException e) {
      throw new RuntimeException(e);
    } catch (InvocationTargetException e) {
      throw new RuntimeException(e);
    }
    Descriptors.Descriptor descriptor = builder.getDescriptorForType();
    return extensionSupport.getAllFieldsForMessage(descriptor);
  }

  /**
   * Search fields for the given name.  Prefers matching short names to full names.
   * @return null if fields does not contain a field with the given name
   */
  public static Descriptors.FieldDescriptor findFieldByName(String fieldName,
      List<Descriptors.FieldDescriptor> fields) {
    // We do this in two passes deliberately, so short names take full precedence.
    for (Descriptors.FieldDescriptor fd : fields) {
      if (fd.getName().equals(fieldName)) return fd;
    }
    for (Descriptors.FieldDescriptor fd : fields) {
      if (fd.getFullName().equals(fieldName)) return fd;
    }
    return null;
  }
}
