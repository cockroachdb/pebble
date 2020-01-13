load("@io_bazel_rules_go//go:def.bzl", "go_library", "go_test")

go_library(
    name = "go_default_library",
    srcs = ["rate.go"],
    importpath = "github.com/cockroachdb/pebble/internal/rate",
    visibility = ["//:__subpackages__"],
)

go_test(
    name = "go_default_test",
    size = "small",
    srcs = ["rate_test.go"],
    embed = [":go_default_library"],
)
