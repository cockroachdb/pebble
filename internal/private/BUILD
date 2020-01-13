load("@io_bazel_rules_go//go:def.bzl", "go_library")

go_library(
    name = "go_default_library",
    srcs = [
        "batch.go",
        "sstable.go",
    ],
    importpath = "github.com/cockroachdb/pebble/internal/private",
    visibility = ["//:__subpackages__"],
    deps = ["//internal/base:go_default_library"],
)
