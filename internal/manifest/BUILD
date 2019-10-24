load("@io_bazel_rules_go//go:def.bzl", "go_library", "go_test")

go_library(
    name = "go_default_library",
    srcs = [
        "version.go",
        "version_edit.go",
    ],
    importpath = "github.com/cockroachdb/pebble/internal/manifest",
    visibility = ["//:__subpackages__"],
    deps = ["//internal/base:go_default_library"],
)

go_test(
    name = "go_default_test",
    srcs = [
        "version_edit_test.go",
        "version_test.go",
    ],
    data = glob(["testdata/**"]) + ["//:testdata"],
    embed = [":go_default_library"],
    deps = [
        "//internal/base:go_default_library",
        "//internal/datadriven:go_default_library",
        "//internal/record:go_default_library",
        "@com_github_kr_pretty//:go_default_library",
    ],
    size = "small",
)
