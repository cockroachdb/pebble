load("@io_bazel_rules_go//go:def.bzl", "go_library", "go_test")

go_library(
    name = "go_default_library",
    srcs = [
        "fragmenter.go",
        "get.go",
        "iter.go",
        "race_off.go",
        "race_on.go",
        "seek.go",
        "tombstone.go",
        "truncate.go",
    ],
    importpath = "github.com/cockroachdb/pebble/internal/rangedel",
    visibility = ["//:__subpackages__"],
    deps = ["//internal/base:go_default_library"],
)

go_test(
    name = "go_default_test",
    size = "small",
    srcs = [
        "fragmenter_test.go",
        "iter_test.go",
        "seek_test.go",
        "truncate_test.go",
    ],
    data = glob(["testdata/**"]),
    embed = [":go_default_library"],
    deps = [
        "//internal/base:go_default_library",
        "//internal/datadriven:go_default_library",
    ],
)
