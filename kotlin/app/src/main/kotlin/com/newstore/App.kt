package com.newstore

fun hello(name: String): String = "Hello, $name!"

class App {
    companion object {
        @JvmStatic
        fun main(args: Array<String>) {
            println(hello("NewStore"))
        }
    }
}
