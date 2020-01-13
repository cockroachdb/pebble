load("@io_bazel_rules_go//go:def.bzl", "go_library")

go_library(
    name = "go_default_library",
    srcs = [
        "doc.go",
        "indent.go",
        "wrap.go",
    ],
    importmap = "github.com/cockroachdb/pebble/vendor/github.com/kr/text",
    importpath = "github.com/kr/text",
    visibility = ["//visibility:public"],
)
