from hello.hello import hello

def test_greeting():
  assert hello("NewStore") == "Hello, NewStore!"
