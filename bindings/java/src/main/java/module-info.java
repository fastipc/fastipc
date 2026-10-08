/**
 * FastIPC for Java: messages and RPC between two processes on one machine, through shared memory.
 *
 * <p>The module calls the native library through the FFM API ({@code java.lang.foreign}), so it needs native access:
 * run with {@code --enable-native-access=io.github.fastipc} on the module path, or
 * {@code --enable-native-access=ALL-UNNAMED} on the class path. See {@link io.github.fastipc}.
 */
module io.github.fastipc {
    exports io.github.fastipc;
}
