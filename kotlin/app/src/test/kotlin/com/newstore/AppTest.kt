package com.newstore

import kotlin.test.Test
import kotlin.test.assertEquals

class AppTest {

  @Test
  fun `displays greeting`() {
    assertEquals("Hello, NewStore!", hello("NewStore"))
  }
}
