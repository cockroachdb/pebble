load("@io_bazel_rules_go//go:def.bzl", "go_library")

go_library(
    name = "go_default_library",
    srcs = [
        "hdr.go",
        "window.go",
    ],
    importmap = "github.com/cockroachdb/pebble/vendor/github.com/codahale/hdrhistogram",
    importpath = "github.com/codahale/hdrhistogram",
    visibility = ["//visibility:public"],
)
