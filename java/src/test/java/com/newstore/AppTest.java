package com.newstore;

import org.junit.jupiter.api.Test;

import static com.newstore.App.hello;
import static org.junit.jupiter.api.Assertions.assertEquals;

public class AppTest {

  @Test
  void displaysGreeting() {
    assertEquals("Hello, NewStore!", hello("NewStore"));
  }
}
