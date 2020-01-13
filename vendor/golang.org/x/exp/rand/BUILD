load("@io_bazel_rules_go//go:def.bzl", "go_library")

go_library(
    name = "go_default_library",
    srcs = [
        "exp.go",
        "normal.go",
        "rand.go",
        "rng.go",
        "uint64.go",
        "uint64_fallback.go",
        "zipf.go",
    ],
    importmap = "github.com/cockroachdb/pebble/vendor/golang.org/x/exp/rand",
    importpath = "golang.org/x/exp/rand",
    visibility = ["//visibility:public"],
)
