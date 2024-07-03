import { hello } from "../src/hello";
import assert from "node:assert";
import { test } from "node:test";

test("greeting", () => {
  assert.equal(hello("NewStore"), "Hello, NewStore!");
});
