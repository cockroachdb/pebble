load("@io_bazel_rules_go//go:def.bzl", "go_library", "go_test")

go_library(
    name = "go_default_library",
    srcs = [
        "config.go",
        "generator.go",
        "history.go",
        "ops.go",
        "options.go",
        "parser.go",
        "test.go",
        "utils.go",
    ],
    importpath = "github.com/cockroachdb/pebble/internal/metamorphic",
    visibility = ["//:__subpackages__"],
    deps = [
        "//:go_default_library",
        "//internal/cache:go_default_library",
        "//internal/private:go_default_library",
        "//internal/randvar:go_default_library",
        "//sstable:go_default_library",
        "//vfs:go_default_library",
        "@org_golang_x_exp//rand:go_default_library",
    ],
)

go_test(
    name = "go_default_test",
    srcs = [
        "generator_test.go",
        "meta_test.go",
        "parser_test.go",
    ],
    data = glob(["testdata/**"]),
    embed = [":go_default_library"],
    deps = [
        "//:go_default_library",
        "//internal/base:go_default_library",
        "//internal/datadriven:go_default_library",
        "//internal/randvar:go_default_library",
        "//vfs:go_default_library",
        "@com_github_kr_pretty//:go_default_library",
        "@com_github_stretchr_testify//require:go_default_library",
        "@org_golang_x_exp//rand:go_default_library",
    ],
    size = "small",
)
