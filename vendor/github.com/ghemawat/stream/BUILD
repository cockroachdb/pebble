load("@io_bazel_rules_go//go:def.bzl", "go_library")

go_library(
    name = "go_default_library",
    srcs = [
        "command.go",
        "filters.go",
        "find.go",
        "firstlast.go",
        "io.go",
        "parallel.go",
        "regexp.go",
        "sample.go",
        "sort.go",
        "stream.go",
        "xargs.go",
    ],
    importmap = "github.com/cockroachdb/pebble/vendor/github.com/ghemawat/stream",
    importpath = "github.com/ghemawat/stream",
    visibility = ["//visibility:public"],
)
