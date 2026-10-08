package io.github.fastipc;

import java.io.IOException;
import java.io.InputStream;
import java.lang.foreign.Arena;
import java.lang.foreign.SymbolLookup;
import java.net.URISyntaxException;
import java.nio.file.AccessDeniedException;
import java.nio.file.FileSystemException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardCopyOption;
import java.security.CodeSource;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.util.ArrayList;
import java.util.HexFormat;
import java.util.List;
import java.util.Locale;

/**
 * Finds and loads the native library, in this order: the file the system property {@value #PROPERTY} names; in a
 * development checkout of the repository (the binding's classes under {@code bindings/java} of a folder with
 * {@code build.zig}), its zig-out build; the jar's library, extracted into a cache folder of the user; the system's
 * search. The library is loaded into the global arena: never unloaded (include/fipc.h, "Unloading the library").
 */
final class NativeLoader {
    static final String PROPERTY = "fastipc.library.path";

    /** The loaded library's symbols and where it came from. */
    record Loaded(SymbolLookup symbols, String description) {
    }

    private NativeLoader() {
    }

    static Loaded load() {
        String os = System.getProperty("os.name", "").toLowerCase(Locale.ROOT);
        String arch = System.getProperty("os.arch", "").toLowerCase(Locale.ROOT);
        boolean windows = os.startsWith("windows");
        boolean macos = os.startsWith("mac") && arch.equals("aarch64");
        boolean linuxArm64 = os.startsWith("linux") && arch.equals("aarch64");
        if (!macos && !linuxArm64
            && (!(windows || os.startsWith("linux")) || !(arch.equals("amd64") || arch.equals("x86_64")))) {
            throw new UnsatisfiedLinkError("FastIPC runs on Linux and Windows on x86-64 and on Linux and macOS on "
                + "arm64, not " + os + " on " + arch);
        }
        String file = windows ? "fastipc.dll" : macos ? "libfastipc.dylib" : "libfastipc.so";
        String platform = windows ? "windows-x86_64" : macos ? "macos-aarch64" : linuxArm64 ? "linux-aarch64"
            : "linux-x86_64";

        String override = System.getProperty(PROPERTY);
        if (override != null && !override.isEmpty()) {
            Path path = Path.of(override).toAbsolutePath();
            if (!Files.isRegularFile(path)) {
                throw new UnsatisfiedLinkError(PROPERTY + " names " + path + ", which isn't a file");
            }
            return open(path);
        }
        Path development = developmentLibrary(file);
        if (development != null) {
            return open(development);
        }
        Path bundled;
        try {
            bundled = extractBundled("native/" + platform + "/" + file, file);
        } catch (IOException e) {
            UnsatisfiedLinkError error = new UnsatisfiedLinkError("can't extract the jar's " + file + ": " + e);
            error.initCause(e);
            throw error;
        }
        if (bundled != null) {
            return open(bundled);
        }
        try {
            return new Loaded(SymbolLookup.libraryLookup(file, Arena.global()), file);
        } catch (IllegalArgumentException e) {
            UnsatisfiedLinkError error = new UnsatisfiedLinkError(file + " not found: not in the jar (native/" + platform
                + "/), no " + PROPERTY + ", and not on the system's search path");
            error.initCause(e);
            throw error;
        }
    }

    private static Loaded open(Path path) {
        try {
            return new Loaded(SymbolLookup.libraryLookup(path, Arena.global()), path.toString());
        } catch (IllegalArgumentException e) {
            UnsatisfiedLinkError error = new UnsatisfiedLinkError("can't load " + path + ": " + e.getMessage());
            error.initCause(e);
            throw error;
        }
    }

    /**
     * The repository's zig-out build of {@code file} when the binding's classes or jar are inside
     * {@code <repository>/bindings/java} (a development checkout); otherwise null.
     */
    static Path developmentLibrary(String file) {
        CodeSource source = NativeLoader.class.getProtectionDomain().getCodeSource();
        if (source == null || source.getLocation() == null) {
            return null;
        }
        Path location;
        try {
            location = Path.of(source.getLocation().toURI()).toAbsolutePath();
        } catch (URISyntaxException | IllegalArgumentException e) {
            return null;
        }
        for (Path dir = location; dir != null && dir.getParent() != null; dir = dir.getParent()) {
            Path parent = dir.getParent();
            if (String.valueOf(dir.getFileName()).equals("java") && String.valueOf(parent.getFileName()).equals("bindings")) {
                Path repository = parent.getParent();
                if (repository == null || !Files.isRegularFile(repository.resolve("build.zig"))) {
                    return null;
                }
                for (String folder : List.of("zig-out/lib", "zig-out/bin")) {
                    Path library = repository.resolve(folder).resolve(file);
                    if (Files.isRegularFile(library)) {
                        return library;
                    }
                }
                return null;
            }
        }
        return null;
    }

    /** The jar's copy of {@code resource}, extracted into a cache folder; null when the jar has none. */
    private static Path extractBundled(String resource, String file) throws IOException {
        byte[] bytes;
        try (InputStream in = NativeLoader.class.getResourceAsStream(resource)) {
            if (in == null) {
                return null;
            }
            bytes = in.readAllBytes();
        }
        IOException failure = null;
        for (Path base : cacheFolders()) {
            try {
                return extract(bytes, file, base);
            } catch (IOException e) {
                failure = e;
            }
        }
        try {
            // No writable cache folder: a new private temporary folder of this process
            Path folder = Files.createTempDirectory("fipc-");
            folder.toFile().deleteOnExit();
            Path target = extract(bytes, file, folder);
            target.toFile().deleteOnExit();
            return target;
        } catch (IOException e) {
            if (failure != null) {
                e.addSuppressed(failure);
            }
            throw e;
        }
    }

    /** The user's cache folders for the library, the preferred first. */
    private static List<Path> cacheFolders() {
        List<Path> folders = new ArrayList<>();
        String localAppData = System.getenv("LOCALAPPDATA");
        String xdgCache = System.getenv("XDG_CACHE_HOME");
        String home = System.getProperty("user.home");
        if (System.getProperty("os.name", "").startsWith("Windows")) {
            if (localAppData != null && !localAppData.isEmpty()) {
                folders.add(Path.of(localAppData, "fipc"));
            }
        } else if (System.getProperty("os.name", "").startsWith("Mac")) {
            if (home != null && !home.isEmpty()) {
                folders.add(Path.of(home, "Library", "Caches", "fipc"));
            }
        } else if (xdgCache != null && xdgCache.startsWith("/")) {
            folders.add(Path.of(xdgCache, "fipc"));
        } else if (home != null && !home.isEmpty()) {
            folders.add(Path.of(home, ".cache", "fipc"));
        }
        return folders;
    }

    /**
     * {@code bytes} as {@code base/<sha256 prefix>/file}: a folder per content, so different versions never share a
     * file, and a file already there is used only when its content is {@code bytes} (it is rewritten otherwise).
     */
    static Path extract(byte[] bytes, String file, Path base) throws IOException {
        byte[] digest = sha256(bytes);
        Path folder = base.resolve(HexFormat.of().formatHex(digest, 0, 8));
        Path target = folder.resolve(file);
        if (holds(target, digest)) {
            return target;
        }
        Files.createDirectories(folder);
        Path temporary = Files.createTempFile(folder, file, ".tmp");
        try {
            Files.write(temporary, bytes);
            try {
                Files.move(temporary, target, StandardCopyOption.ATOMIC_MOVE, StandardCopyOption.REPLACE_EXISTING);
            } catch (AccessDeniedException e) {
                // Windows: another process has the library loaded, so it can't be replaced; it is fine if it holds it
                if (!holds(target, digest)) {
                    throw e;
                }
            }
        } finally {
            Files.deleteIfExists(temporary);
        }
        if (!holds(target, digest)) {
            throw new FileSystemException(target.toString(), null, "the extracted library doesn't match the jar's");
        }
        return target;
    }

    private static boolean holds(Path file, byte[] digest) throws IOException {
        return Files.isRegularFile(file) && MessageDigest.isEqual(sha256(Files.readAllBytes(file)), digest);
    }

    private static byte[] sha256(byte[] bytes) {
        try {
            return MessageDigest.getInstance("SHA-256").digest(bytes);
        } catch (NoSuchAlgorithmException e) {
            throw new AssertionError("every JDK has SHA-256", e);
        }
    }
}
