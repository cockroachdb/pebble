load("@io_bazel_rules_go//go:def.bzl", "go_library", "go_test")

go_library(
    name = "go_default_library",
    srcs = [
        "log_writer.go",
        "record.go",
    ],
    importpath = "github.com/cockroachdb/pebble/internal/record",
    visibility = ["//:__subpackages__"],
    deps = ["//internal/crc:go_default_library"],
)

go_test(
    name = "go_default_test",
    size = "small",
    srcs = [
        "log_writer_test.go",
        "record_test.go",
    ],
    embed = [":go_default_library"],
    deps = [
        "//vendor/golang.org/x/exp/rand:go_default_library",
        "//vfs:go_default_library",
    ],
)
