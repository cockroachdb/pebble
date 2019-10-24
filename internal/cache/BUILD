load("@io_bazel_rules_go//go:def.bzl", "go_library", "go_test")

go_library(
    name = "go_default_library",
    srcs = [
        "alloc.go",
        "clockpro.go",
    ],
    importpath = "github.com/cockroachdb/pebble/internal/cache",
    visibility = ["//:__subpackages__"],
    deps = ["//vendor/golang.org/x/exp/rand:go_default_library"],
)

go_test(
    name = "go_default_test",
    size = "small",
    srcs = [
        "alloc_test.go",
        "clockpro_test.go",
    ],
    data = glob(["testdata/**"]),
    embed = [":go_default_library"],
    deps = ["//vendor/github.com/stretchr/testify/require:go_default_library"],
)
