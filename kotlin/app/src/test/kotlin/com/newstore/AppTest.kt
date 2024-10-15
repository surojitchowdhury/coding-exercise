package com.newstore

import kotlin.test.Test
import kotlin.test.assertEquals

class AppTest {

    @Test
    fun `displays greeting`() {
        assertEquals(hello("NewStore"), "Hello, NewStore!")
    }
}
