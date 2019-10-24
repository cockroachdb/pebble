load("@io_bazel_rules_go//go:def.bzl", "go_library", "go_test")

go_library(
    name = "go_default_library",
    srcs = [
        "deck.go",
        "flag.go",
        "rand.go",
        "randvar.go",
        "skewed_latest.go",
        "uniform.go",
        "weighted.go",
        "zipf.go",
    ],
    importpath = "github.com/cockroachdb/pebble/internal/randvar",
    visibility = ["//:__subpackages__"],
    deps = ["@org_golang_x_exp//rand:go_default_library"],
)

go_test(
    name = "go_default_test",
    srcs = [
        "deck_test.go",
        "skewed_latest_test.go",
        "weighted_test.go",
        "zipf_test.go",
    ],
    embed = [":go_default_library"],
    size = "small",
)
