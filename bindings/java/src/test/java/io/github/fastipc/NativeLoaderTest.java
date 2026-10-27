package io.github.fastipc;

import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.nio.file.Files;
import java.nio.file.Path;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

/** Finding the library: the development fallback, and the extraction of a jar's copy into a cache folder. */
class NativeLoaderTest {
    @Test
    void developmentCheckoutUsesZigOut() {
        String os = System.getProperty("os.name");
        String file = os.startsWith("Windows") ? "fastipc.dll"
            : os.startsWith("Mac") ? "libfastipc.dylib" : "libfastipc.so";
        Path library = NativeLoader.developmentLibrary(file);
        assertTrue(library != null && library.startsWith(TestSupport.repository().resolve("zig-out")), "" + library);
        assertEquals(library.toString(), Fipc.libraryPath());
    }

    @Test
    void extractionIsPerContentAndChecked(@TempDir Path cache) throws Exception {
        byte[] one = TestSupport.pattern(5000);
        Path first = NativeLoader.extract(one, "libfastipc.so", cache);
        assertArrayEquals(one, Files.readAllBytes(first));
        assertEquals(first, NativeLoader.extract(one, "libfastipc.so", cache)); // reused

        // A changed file is rewritten before it is used
        Files.write(first, new byte[] {1, 2, 3});
        assertEquals(first, NativeLoader.extract(one, "libfastipc.so", cache));
        assertArrayEquals(one, Files.readAllBytes(first));

        // Other content, another folder
        byte[] two = TestSupport.pattern(5001);
        Path second = NativeLoader.extract(two, "libfastipc.so", cache);
        assertNotEquals(first.getParent(), second.getParent());
        assertArrayEquals(one, Files.readAllBytes(first));
        try (var files = Files.list(first.getParent())) {
            assertEquals(1, files.count(), "no temporary file left behind");
        }
    }
}
