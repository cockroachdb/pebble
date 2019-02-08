// Copyright 2011 The LevelDB-Go Authors. All rights reserved.
// Use of this source code is governed by a BSD-style
// license that can be found in the LICENSE file.

// This program adds N lines from infile to a leveldb table at outfile.
// The h.txt infile was generated via:
// cat hamlet-act-1.txt | tr '[:upper:]' '[:lower:]' | grep -o -E '\w+' | sort | uniq -c > infile
//
// To build and run:
// g++ make-table.cc -lleveldb && ./a.out

#include <fstream>
#include <iostream>
#include <string>

#include "rocksdb/env.h"
#include "rocksdb/filter_policy.h"
#include "rocksdb/slice_transform.h"
#include "rocksdb/sst_file_writer.h"
#include "rocksdb/table.h"

const char* infile = "h.txt";
const char* outfiles[] = {
  "h.ldb", // omitted
  "h.sst",
  "h.no-compression.sst",
  "h.block-bloom.no-compression.sst",
  "h.table-bloom.no-compression.sst",
  "h.block-bloom.no-compression.prefix_extractor.sst", // omitted
  "h.table-bloom.no-compression.prefix_extractor.sst", // omitted
  "h.block-bloom.no-compression.prefix_extractor.no_whole_key_filter.sst", // omitted
  "h.table-bloom.no-compression.prefix_extractor.no_whole_key_filter.sst",
};

// A dummy prefix extractor that cuts off the last two bytes for keys of
// length three or over. This is not a valid prefix extractor and barely
// enough to do a little bit of unit testing.
//
// TODO(tbg): write some test infra using CockroachDB MVCC data.
class PrefixExtractor : public rocksdb::SliceTransform {
 public:
  PrefixExtractor() {}

  virtual const char* Name() const { return "leveldb.BytewiseComparator"; }

  virtual rocksdb::Slice Transform(const rocksdb::Slice& src) const {
    auto sl = rocksdb::Slice(src.data(), src.size());
    return sl;
  }

  virtual bool InDomain(const rocksdb::Slice& src) const { return true; }
};

int write() {
  for (int i = 0; i < 9; ++i) {
    if (i == 0 || i == 5 || i == 6 || i == 7) {
        // TODO(peter): instill sanity.
        continue;
    }
    const char* outfile = outfiles[i];
    rocksdb::Status status;

    rocksdb::BlockBasedTableOptions table_options;
    // This bool defaults to true, so set it to false explicitly when we're not
    // also enabling bloom filters. Otherwise, the pebble tests need to specify
    // the corresponding property, which is awkward.
    table_options.whole_key_filtering = (i >= 3);

    if (i == 0) {
      table_options.format_version = 0;
    }
    if (i >= 3) {
      table_options.filter_policy.reset(rocksdb::NewBloomFilterPolicy(10, (i % 2) == 1));
    }

    rocksdb::Options options;
    if (i >= 5) {
      options.prefix_extractor.reset(new PrefixExtractor);
      table_options.whole_key_filtering = true;
    }
    if (i >= 7) {
      table_options.whole_key_filtering = false;
    }

    options.table_factory.reset(rocksdb::NewBlockBasedTableFactory(table_options));
    if (i >= 2) {
      options.compression = rocksdb::kNoCompression;
    }

    std::unique_ptr<rocksdb::SstFileWriter> tb(new rocksdb::SstFileWriter({}, options));
    status = tb->Open(outfile);
    if (!status.ok()) {
      std::cerr << "SstFileWriter::Open: " << status.ToString() << std::endl;
      return 1;
    }
    std::ifstream in(infile);
    std::string s;
    for (int i = 0; getline(in, s); i++) {
      std::string key(s, 8);
      std::string val(s, 0, 7);
      val = val.substr(1 + val.rfind(' '));
      tb->Put(key.c_str(), val.c_str());
    }

    rocksdb::ExternalSstFileInfo info;
    status = tb->Finish(&info);
    if (!status.ok()) {
      std::cerr << "TableBuilder::Finish: " << status.ToString() << std::endl;
      return 1;
    }

    std::cout << outfile << ": wrote " << info.num_entries << " entries, " << info.file_size << "b" << std::endl;
  }
  return 0;
}

int main(int argc, char** argv) {
  return write();
}
