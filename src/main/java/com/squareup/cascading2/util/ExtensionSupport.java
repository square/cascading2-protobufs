package com.squareup.cascading2.util;

import com.google.protobuf.Descriptors;
import com.google.protobuf.Message;
import java.io.Serializable;
import java.util.List;

import static com.google.protobuf.Descriptors.FieldDescriptor;

/** Provides introspection of {@link Message} with support for extensions, if available. */
public interface ExtensionSupport extends Serializable {

  /**
   * An implementation which does <i>not</i> return extension fields.
   *
   * Equivalent to {@link Message#getAllFields()}.  Suitable for use as a default.
   */
  final ExtensionSupport NONE = new ExtensionSupport() {
    @Override
    public List<FieldDescriptor> getAllFieldsForMessage(Descriptors.Descriptor messageDescriptor) {
      return messageDescriptor.getFields();
    }
  };

  /**
   * Obtain descriptors for all fields on the message, including extension fields if that the
   * implementation has that information.  The returned descriptors are suitable for use with
   * {@link Message#getField(FieldDescriptor)} etc.
   *
   * Note: this is preferable to {@link Message#getAllFields()}, which does not return extension
   * fields.
   */
  List<FieldDescriptor> getAllFieldsForMessage(Descriptors.Descriptor messageDescriptor);
}
