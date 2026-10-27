# Java examples

The Java server and client of the repository's [README](../../README.md#java), a broadcast's publisher and
subscriber, a work queue's producer and consumer, and a service's server and client, using the binding `io.github.fastipc:fipc` (Java 22+). Each file
is a program the source launcher runs as it is, with the jar on the class path:

```bash
java --enable-native-access=ALL-UNNAMED -cp fipc-1.0.0.jar Server.java       # in one terminal
java --enable-native-access=ALL-UNNAMED -cp fipc-1.0.0.jar Client.java       # in another: PING
java --enable-native-access=ALL-UNNAMED -cp fipc-1.0.0.jar Publisher.java    # in one terminal
java --enable-native-access=ALL-UNNAMED -cp fipc-1.0.0.jar Subscriber.java   # in another: tick 1, tick 2, tick 3
java --enable-native-access=ALL-UNNAMED -cp fipc-1.0.0.jar Producer.java     # in one terminal
java --enable-native-access=ALL-UNNAMED -cp fipc-1.0.0.jar Consumer.java     # in another: job 1, job 2, job 3
java --enable-native-access=ALL-UNNAMED -cp fipc-1.0.0.jar ServiceServer.java  # in one terminal
java --enable-native-access=ALL-UNNAMED -cp fipc-1.0.0.jar ServiceClient.java  # in another: PING 1, PING 2, PING 3
```

The pair works with every other language's server or client: the message `ping`, answered with its uppercase,
`PING`. The publisher and subscriber make the broadcast's exchange on the broadcast `my_broadcast`: the
publisher waits for a subscriber, sends `tick 1` to `tick 3` and closes, which ends the broadcast. The producer and
consumer make the work queue's exchange on the queue `my_queue`: the producer creates the queue, joins it as a
producer, sends `job 1` to `job 3`, which wait in the queue, waits for the consumer to join, and closes its producer
and the queue, which ends it. The service's server and client make the service's exchange on the service
`my_service`: the server creates the service, joins it as its server, answers three requests with their uppercase and
closes its server and the service, which ends it; the client joins, calls `ping 1` to `ping 3` (each `call` sends a
request and returns its reply) and prints the replies. In a checkout,
`bindings/java/gradlew -p bindings/java jar` builds the jar in `bindings/java/build/libs`, which loads the checkout's
`zig-out` build. The repository's interop test (`python devtool.py test interop`) runs the server, client, publisher,
subscriber, producer, consumer, service server and service client against every language's examples.
