load("@io_bazel_rules_go//go:def.bzl", "go_library", "go_test")

go_library(
    name = "go_default_library",
    srcs = [
        "block.go",
        "comparer.go",
        "filter.go",
        "internal.go",
        "options.go",
        "properties.go",
        "raw_block.go",
        "reader.go",
        "table.go",
        "unsafe.go",
        "writer.go",
    ],
    importpath = "github.com/cockroachdb/pebble/sstable",
    visibility = ["//visibility:public"],
    deps = [
        "//internal/base:go_default_library",
        "//internal/cache:go_default_library",
        "//internal/crc:go_default_library",
        "//internal/private:go_default_library",
        "//internal/rangedel:go_default_library",
        "//vendor/github.com/golang/snappy:go_default_library",
        "//vfs:go_default_library",
    ],
)

filegroup(
    name = "testdata",
    data = glob(["testdata/**"]),
    visibility = ["//:__subpackages__"],
)

go_test(
    name = "go_default_test",
    size = "small",
    srcs = [
        "block_test.go",
        "properties_test.go",
        "reader_test.go",
        "table_test.go",
        "unsafe_test.go",
        "writer_fixture_test.go",
        "writer_test.go",
    ],
    data = [":testdata"],
    embed = [":go_default_library"],
    deps = [
        "//bloom:go_default_library",
        "//internal/base:go_default_library",
        "//internal/cache:go_default_library",
        "//internal/datadriven:go_default_library",
        "//internal/rangedel:go_default_library",
        "//vendor/github.com/kr/pretty:go_default_library",
        "//vendor/github.com/stretchr/testify/require:go_default_library",
        "//vendor/golang.org/x/exp/rand:go_default_library",
        "//vfs:go_default_library",
    ],
)
