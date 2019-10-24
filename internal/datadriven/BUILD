load("@io_bazel_rules_go//go:def.bzl", "go_library", "go_test")

go_library(
    name = "go_default_library",
    srcs = [
        "datadriven.go",
        "line_scanner.go",
        "test_data_reader.go",
    ],
    importpath = "github.com/cockroachdb/pebble/internal/datadriven",
    visibility = ["//:__subpackages__"],
)

go_test(
    name = "go_default_test",
    srcs = ["datadriven_test.go"],
    embed = [":go_default_library"],
    size = "small",
)
