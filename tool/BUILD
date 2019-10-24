load("@io_bazel_rules_go//go:def.bzl", "go_library", "go_test")

go_library(
    name = "go_default_library",
    srcs = [
        "db.go",
        "manifest.go",
        "sstable.go",
        "tool.go",
        "util.go",
        "wal.go",
    ],
    importpath = "github.com/cockroachdb/pebble/tool",
    visibility = ["//visibility:public"],
    deps = [
        "//:go_default_library",
        "//bloom:go_default_library",
        "//internal/base:go_default_library",
        "//internal/manifest:go_default_library",
        "//internal/record:go_default_library",
        "//sstable:go_default_library",
        "//vendor/github.com/spf13/cobra:go_default_library",
        "//vfs:go_default_library",
    ],
)

go_test(
    name = "go_default_test",
    size = "small",
    srcs = [
        "data_test.go",
        "db_test.go",
        "manifest_test.go",
        "sstable_test.go",
        "wal_test.go",
    ],
    data = glob(["testdata/**"]) + [
        "//:testdata",
        "//sstable:testdata",
    ],
    embed = [":go_default_library"],
    deps = [
        "//internal/base:go_default_library",
        "//internal/datadriven:go_default_library",
        "//vendor/github.com/spf13/cobra:go_default_library",
        "//vfs:go_default_library",
    ],
)
