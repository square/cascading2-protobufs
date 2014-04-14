package com.squareup.cascading2.function;

import cascading.flow.FlowProcess;
import cascading.operation.BaseOperation;
import cascading.operation.Function;
import cascading.operation.FunctionCall;
import cascading.tuple.Fields;
import cascading.tuple.Tuple;
import com.google.protobuf.ByteString;
import java.io.UnsupportedEncodingException;

/**
 *  Convert a ByteString {@link com.google.protobuf.ByteString} to a decoded string of the given encoding.
 *  Defaults to UTF8 if no encoding is provided.
 */
public class DecodeByteString extends BaseOperation implements Function {
  private final String encoding;

  public DecodeByteString(String charsetName, String fieldName) {
    super(1, new Fields(fieldName));
    encoding = charsetName;
  }

  public DecodeByteString(String fieldName) {
    this("UTF8", fieldName);
  }

  @Override public void operate(FlowProcess flowProcess, FunctionCall functionCall) {
    Tuple result;
    try {
      ByteString arg = (ByteString) functionCall.getArguments().getObject(0);
      functionCall.getOutputCollector().add(new Tuple(arg == null ? null : arg.toString(encoding)));
    } catch (UnsupportedEncodingException e) {
      throw new RuntimeException("Unsupported encoding exception: ", e);
    }
  }
}
