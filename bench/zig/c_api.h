/*
 * The C API the benchmark suite (bench/zig) calls: the build translates this header into the module "c" (the
 * build-system form of @cImport). The suite loads the library at run time and runs against any build that exports
 * this API: this tree's, or another revision's that speaks include/fipc.h.
 */

#include "fipc.h"
