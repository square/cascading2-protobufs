package com.squareup.cascading2.function;

import com.google.protobuf.Descriptors;
import com.squareup.cascading2.generated.Example;
import com.squareup.cascading2.util.ExtensionSupport;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import junit.framework.TestCase;

final class TestExtensionSupport {

  static final Example.Coffee COFFEE = Example.Coffee.newBuilder().setSugar(true).build();

  static final Example.Drink COFFEE_DRINK = Example.Drink.newBuilder()
      .setOunces(20)
      .setExtension(Example.coffee, COFFEE)
      .build();

  static final Example.Tea TEA = Example.Tea.newBuilder().setMilk(true).build();

  static Example.Drink TEA_DRINK = Example.Drink.newBuilder()
      .setOunces(16)
      .setExtension(Example.tea, TEA)
      .build();

  static final Example.DrinkOrder TEA_AND_TWO_COFFEES = Example.DrinkOrder.newBuilder()
      .setName("Nathan")
      .addExtension(Example.drink, TEA_DRINK)
      .addExtension(Example.drink, COFFEE_DRINK)
      .addExtension(Example.drink, COFFEE_DRINK)
      .build();

  static final ExtensionSupport DRINK_EXTENSIONS = new ExtensionSupport() {
    @Override public List<Descriptors.FieldDescriptor> getAllFieldsForMessage(
        Descriptors.Descriptor messageDescriptor) {
      List<Descriptors.FieldDescriptor> fields = new ArrayList<Descriptors.FieldDescriptor>();
      fields.addAll(messageDescriptor.getFields());
      if (messageDescriptor == Example.Drink.getDescriptor()) {
        fields.add(Example.tea.getDescriptor());
        fields.add(Example.coffee.getDescriptor());
      }
      return Collections.unmodifiableList(fields);
    }
  };

  static final ExtensionSupport DRINK_ORDER_EXTENSIONS = new ExtensionSupport() {
    @Override public List<Descriptors.FieldDescriptor> getAllFieldsForMessage(
        Descriptors.Descriptor messageDescriptor) {
      TestCase.assertTrue(messageDescriptor == Example.DrinkOrder.getDescriptor());
      List<Descriptors.FieldDescriptor> fields = new ArrayList<Descriptors.FieldDescriptor>();
      fields.addAll(Example.DrinkOrder.getDescriptor().getFields());
      fields.add(Example.drink.getDescriptor());
      return Collections.unmodifiableList(fields);
    }
  };

  private TestExtensionSupport() {
  }
}
