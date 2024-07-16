import { hello } from "../src/hello";

test("greeting", () => {
  expect(hello("NewStore")).toBe("Hello, NewStore!");
});
