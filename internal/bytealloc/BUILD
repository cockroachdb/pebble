load("@io_bazel_rules_go//go:def.bzl", "go_library")

go_library(
    name = "go_default_library",
    srcs = ["bytealloc.go"],
    importpath = "github.com/cockroachdb/pebble/internal/bytealloc",
    visibility = ["//:__subpackages__"],
    deps = ["//internal/rawalloc:go_default_library"],
)
