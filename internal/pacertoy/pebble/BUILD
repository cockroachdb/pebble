load("@io_bazel_rules_go//go:def.bzl", "go_binary", "go_library")

go_library(
    name = "go_default_library",
    srcs = ["main.go"],
    importpath = "github.com/cockroachdb/pebble/internal/pacertoy/pebble",
    visibility = ["//visibility:private"],
    deps = [
        "//internal/rate:go_default_library",
        "//vendor/golang.org/x/exp/rand:go_default_library",
    ],
)

go_binary(
    name = "pebble",
    embed = [":go_default_library"],
    visibility = ["//:__subpackages__"],
)
