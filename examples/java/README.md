# Java examples

The Java server and client of the repository's [README](../../README.md#java), using the binding
`io.github.fastipc:fipc` (Java 22+). Each file is a program the source launcher runs as it is, with the jar on
the class path:

```bash
java --enable-native-access=ALL-UNNAMED -cp fipc-1.0.0.jar Server.java   # in one terminal
java --enable-native-access=ALL-UNNAMED -cp fipc-1.0.0.jar Client.java   # in another: PING
```

The pair works with every other language's server or client: an RPC request with opcode 1 and the payload `ping`,
answered with `PING`. In a checkout, `bindings/java/gradlew -p bindings/java jar` builds the jar in
`bindings/java/build/libs`, which loads the checkout's `zig-out` build. The repository's interop test (`python
devtool.py test interop`) runs both against every language's examples.
