load("@io_bazel_rules_go//go:def.bzl", "go_binary", "go_library")

go_library(
    name = "go_default_library",
    srcs = [
        "badger_disabled.go",
        "boltdb_disabled.go",
        "db.go",
        "main.go",
        "mvcc.go",
        "random.go",
        "rocksdb_disabled.go",
        "scan.go",
        "sync.go",
        "test.go",
        "util.go",
        "ycsb.go",
    ],
    importpath = "github.com/cockroachdb/pebble/cmd/pebble",
    visibility = ["//visibility:private"],
    deps = [
        "//:go_default_library",
        "//internal/ackseq:go_default_library",
        "//internal/base:go_default_library",
        "//internal/bytealloc:go_default_library",
        "//internal/randvar:go_default_library",
        "//internal/rate:go_default_library",
        "//tool:go_default_library",
        "//vendor/github.com/codahale/hdrhistogram:go_default_library",
        "//vendor/github.com/spf13/cobra:go_default_library",
        "//vendor/golang.org/x/exp/rand:go_default_library",
    ],
)

go_binary(
    name = "pebble",
    embed = [":go_default_library"],
    visibility = ["//visibility:public"],
)
