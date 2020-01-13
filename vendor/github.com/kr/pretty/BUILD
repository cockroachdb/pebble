load("@io_bazel_rules_go//go:def.bzl", "go_library")

go_library(
    name = "go_default_library",
    srcs = [
        "diff.go",
        "formatter.go",
        "pretty.go",
        "zero.go",
    ],
    importmap = "github.com/cockroachdb/pebble/vendor/github.com/kr/pretty",
    importpath = "github.com/kr/pretty",
    visibility = ["//visibility:public"],
    deps = ["//vendor/github.com/kr/text:go_default_library"],
)
