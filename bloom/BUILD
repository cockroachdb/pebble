load("@io_bazel_rules_go//go:def.bzl", "go_library", "go_test")

go_library(
    name = "go_default_library",
    srcs = ["bloom.go"],
    importpath = "github.com/cockroachdb/pebble/bloom",
    visibility = ["//visibility:public"],
    deps = ["//internal/base:go_default_library"],
)

go_test(
    name = "go_default_test",
    size = "small",
    srcs = ["bloom_test.go"],
    embed = [":go_default_library"],
    deps = ["//internal/base:go_default_library"],
)
