load("@io_bazel_rules_go//go:def.bzl", "go_library")

go_library(
    name = "go_default_library",
    srcs = [
        "rawalloc.go",
        "rawalloc.s",
        "rawalloc_32bit.go",
        "rawalloc_64bit.go",
        "rawalloc_go1.9.go",
    ],
    importpath = "github.com/cockroachdb/pebble/internal/rawalloc",
    visibility = ["//:__subpackages__"],
)
