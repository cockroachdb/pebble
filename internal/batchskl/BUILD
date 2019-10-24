load("@io_bazel_rules_go//go:def.bzl", "go_library", "go_test")

go_library(
    name = "go_default_library",
    srcs = [
        "iterator.go",
        "skl.go",
    ],
    importpath = "github.com/cockroachdb/pebble/internal/batchskl",
    visibility = ["//:__subpackages__"],
    deps = [
        "//internal/base:go_default_library",
        "@org_golang_x_exp//rand:go_default_library",
    ],
)

go_test(
    name = "go_default_test",
    srcs = ["skl_test.go"],
    embed = [":go_default_library"],
    deps = [
        "//internal/base:go_default_library",
        "@com_github_stretchr_testify//require:go_default_library",
        "@org_golang_x_exp//rand:go_default_library",
    ],
    size = "small",
)
