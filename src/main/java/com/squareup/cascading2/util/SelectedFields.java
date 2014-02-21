package com.squareup.cascading2.util;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.List;

import static com.google.protobuf.Descriptors.FieldDescriptor;

/** A subset of field descriptors for a message. */
public final class SelectedFields implements Serializable {
  private final String messageClassName;
  private final ExtensionSupport extensionSupport;
  private final String[] selectedFieldNames;

  /**
   * Memoized field list.  Must be transient because {@link FieldDescriptor}.
   * Do not access directly.
   * @see #get()
   */
  private transient List<FieldDescriptor> selectedFields;

  public SelectedFields(String messageClassName, ExtensionSupport extensionSupport,
      String[] selectedFieldNames) {
    this.messageClassName = messageClassName;
    this.extensionSupport = extensionSupport;
    this.selectedFieldNames = selectedFieldNames;
  }

  public List<FieldDescriptor> get() {
    if (selectedFields == null) {
      Class messageClass;
      try {
        messageClass = Class.forName(messageClassName);
      } catch (ClassNotFoundException e) {
        throw new RuntimeException(e);
      }
      List<FieldDescriptor> allFields = Util.getFields(messageClass, extensionSupport);
      if (selectedFieldNames == null) {
        selectedFields = allFields;
      } else {
        List<FieldDescriptor> filteredFields = new ArrayList<FieldDescriptor>();
        for (String fieldName : selectedFieldNames) {
          FieldDescriptor field = Util.findFieldByName(fieldName, allFields);
          if (field != null) filteredFields.add(field);
        }
        selectedFields = filteredFields;
      }
    }
    return selectedFields;
  }

  /** @return null if no known field has the given name. */
  public FieldDescriptor findByName(String fieldName) {
    return Util.findFieldByName(fieldName, get());
  }
}
