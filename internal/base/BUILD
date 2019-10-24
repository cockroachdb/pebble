load("@io_bazel_rules_go//go:def.bzl", "go_library", "go_test")

go_library(
    name = "go_default_library",
    srcs = [
        "cleaner.go",
        "comparer.go",
        "error.go",
        "filenames.go",
        "internal.go",
        "iterator.go",
        "merger.go",
        "options.go",
    ],
    importpath = "github.com/cockroachdb/pebble/internal/base",
    visibility = ["//:__subpackages__"],
    deps = ["//vfs:go_default_library"],
)

go_test(
    name = "go_default_test",
    srcs = [
        "comparer_test.go",
        "filenames_test.go",
        "internal_test.go",
    ],
    embed = [":go_default_library"],
    deps = [
        "//vfs:go_default_library",
        "@org_golang_x_exp//rand:go_default_library",
    ],
    size = "small",
)
