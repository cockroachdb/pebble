load("@io_bazel_rules_go//go:def.bzl", "go_library", "go_test")

go_library(
    name = "go_default_library",
    srcs = [
        "clone.go",
        "dir_unix.go",
        "dir_windows.go",
        "fadvise_generic.go",
        "fadvise_linux.go",
        "file_lock_generic.go",
        "file_lock_unix.go",
        "file_lock_windows.go",
        "mem_fs.go",
        "preallocate_darwin.go",
        "preallocate_generic.go",
        "preallocate_linux.go",
        "syncing_file.go",
        "syncing_file_generic.go",
        "syncing_file_linux.go",
        "vfs.go",
    ],
    importpath = "github.com/cockroachdb/pebble/vfs",
    visibility = ["//visibility:public"],
    deps = select({
        "@io_bazel_rules_go//go/platform:android": [
            "@org_golang_x_sys//unix:go_default_library",
        ],
        "@io_bazel_rules_go//go/platform:linux": [
            "@org_golang_x_sys//unix:go_default_library",
        ],
        "//conditions:default": [],
    }),
)

go_test(
    name = "go_default_test",
    srcs = [
        "file_lock_test.go",
        "mem_fs_test.go",
        "syncing_file_linux_test.go",
        "syncing_file_test.go",
        "vfs_test.go",
    ],
    data = glob(["testdata/**"]),
    embed = [":go_default_library"],
    deps = ["//internal/datadriven:go_default_library"],
    size = "small",
)
