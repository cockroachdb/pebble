load("@io_bazel_rules_go//go:def.bzl", "go_library", "go_test")

go_library(
    name = "go_default_library",
    srcs = ["lint.go"],
    importpath = "github.com/cockroachdb/pebble/internal/lint",
    visibility = ["//:__subpackages__"],
)

go_test(
    name = "go_default_test",
    srcs = ["lint_test.go"],
    embed = [":go_default_library"],
    deps = ["@com_github_ghemawat_stream//:go_default_library"],
    size = "small",
)
