package com.squareup.cascading2.function;

import cascading.flow.FlowProcess;
import cascading.operation.BaseOperation;
import cascading.operation.Function;
import cascading.operation.FunctionCall;
import cascading.operation.OperationCall;
import cascading.tuple.Fields;
import cascading.tuple.Tuple;
import com.google.protobuf.Descriptors;
import com.google.protobuf.Message;
import com.squareup.cascading2.util.Util;
import java.util.ArrayList;
import java.util.List;

/**
 * Extract one of more fields from a nested Protobuf object. The fields to extract are specified in
 * field1.field2.field3 syntax. If nulls are encountered along one of these paths, a null is emitted
 * in the resultant tuple.
 *
 * Repeated fields are NOT supported with this function.
 */
public class ExtractProto extends BaseOperation implements Function {
  private final String messageClassName;
  private transient List<List<Descriptors.FieldDescriptor>> paths;
  private final String[] stringPaths;

  public ExtractProto(Class<? extends Message> messageClass, String... paths) {
    super(1, deriveFieldNames(paths));
    stringPaths = paths;
    messageClassName = messageClass.getName();

    for (String path : paths) {
      String[] segments = path.split("\\.");

      Descriptors.Descriptor
          cur = Util.builderFromMessageClass(messageClass.getName()).getDescriptorForType();

      List<Descriptors.FieldDescriptor> descriptors = new ArrayList<Descriptors.FieldDescriptor>();

      for (int i = 0; i < segments.length - 1; i++) {
        Descriptors.FieldDescriptor fieldDesc = cur.findFieldByName(segments[i]);
        if (fieldDesc == null) {
          throw new IllegalArgumentException("Can't find a field named " + segments[i]
              + " in struct " + cur.getName() +". Full path: " + path);
        }
        if (fieldDesc.isRepeated()) {
          throw new IllegalArgumentException("Cannot extract through repeated field " + segments[i]
              + " in struct " + cur.getName() +". Full path: " + path);
        }
        descriptors.add(fieldDesc);
        cur = fieldDesc.getMessageType();
      }

      String lastSegment = segments[segments.length - 1];
      Descriptors.FieldDescriptor lastFieldDesc = cur.findFieldByName(lastSegment);
      if (lastFieldDesc == null) {
        throw new IllegalArgumentException("Can't find a field named " + lastSegment
            + " in struct " + cur.getName() +". Full path: " + path);
      }
      if (lastFieldDesc.isRepeated()) {
        throw new IllegalArgumentException("Cannot extract repeated field " + lastSegment
            + " in struct " + cur.getName() +". Full path: " + path);
      }
    }
  }

  private static Fields deriveFieldNames(String[] paths) {
    return new Fields(paths);
  }

  @Override
  public void prepare(FlowProcess flowProcess, OperationCall operationCall) {
    this.paths = new ArrayList<List<Descriptors.FieldDescriptor>>();

    for (String path : stringPaths) {
      String[] segments = path.split("\\.");

      Descriptors.Descriptor cur = Util.builderFromMessageClass(messageClassName).getDescriptorForType();

      List<Descriptors.FieldDescriptor> descriptors = new ArrayList<Descriptors.FieldDescriptor>();

      for (int i = 0; i < segments.length - 1; i++) {
        Descriptors.FieldDescriptor fieldDesc = cur.findFieldByName(segments[i]);
        if (fieldDesc == null) {
          throw new IllegalArgumentException("Can't find a field named " + segments[i]
              + " in struct " + cur.getName() +". Full path: " + path);
        }
        if (fieldDesc.isRepeated()) {
          throw new IllegalArgumentException("Cannot extract through repeated field " + segments[i]
              + " in struct " + cur.getName() +". Full path: " + path);
        }
        descriptors.add(fieldDesc);
        cur = fieldDesc.getMessageType();
      }

      String lastSegment = segments[segments.length - 1];
      Descriptors.FieldDescriptor lastFieldDesc = cur.findFieldByName(lastSegment);
      if (lastFieldDesc == null) {
        throw new IllegalArgumentException("Can't find a field named " + lastSegment
            + " in struct " + cur.getName() +". Full path: " + path);
      }
      if (lastFieldDesc.isRepeated()) {
        throw new IllegalArgumentException("Cannot extract repeated field " + lastSegment
            + " in struct " + cur.getName() +". Full path: " + path);
      }

      descriptors.add(lastFieldDesc);
      this.paths.add(descriptors);
    }
  }

  @Override public void operate(FlowProcess flowProcess, FunctionCall functionCall) {
    Message msg = (Message)functionCall.getArguments().getObject(0);

    Tuple t = new Tuple();

    OUTER: for (List<Descriptors.FieldDescriptor> fieldDescPath : paths) {
      Message cur = msg;

      if (msg == null) {
        t.add(null);
        continue;
      }

      // walk the descriptor path, advancing "cur" to each Message in the path
      // note that we don't consume the final element this way, as that corresponds to the "value"
      // field at the end of the hierarchy and might not be a Message.
      for (int i = 0; i < fieldDescPath.size() - 1; i++) {
        Descriptors.FieldDescriptor fieldDesc = fieldDescPath.get(i);
        // if the current Message doesn't have this field set, then we're not going to get to the
        // end and we should abort after adding a null to the tuple.
        if (!cur.hasField(fieldDesc)) {
          t.add(null);
          continue OUTER;
        }
        // otherwise, replace cur with the object we just pulled out
        cur = (Message) cur.getField(fieldDesc);
      }

      // we've advanced cur all the way to the last Message in the path. the last path element
      // refers to the value, so let's just get what we need from the Message.
      Descriptors.FieldDescriptor desc = fieldDescPath.get(fieldDescPath.size() - 1);
      if (desc.getJavaType() == Descriptors.FieldDescriptor.JavaType.ENUM) {
        t.add(((Descriptors.EnumValueDescriptor)cur.getField(desc)).getNumber());
      } else {
        t.add(cur.getField(desc));
      }
    }

    // yield the final tuple
    functionCall.getOutputCollector().add(t);
  }
}
