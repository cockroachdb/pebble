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
#include "rocksdb/sst_file_writer.h"
#include "rocksdb/table.h"

const int N = 1000000;
const char* infile = "h.txt";
const char* outfile = "h.sst";
// const char* outfile = "h.no-compression.sst";
// const char* outfile = "h.bloom.no-compression.sst";

int write() {
  rocksdb::Status status;

  rocksdb::BlockBasedTableOptions table_options;
  // The original LevelDB compatible format. We explicitly set the checksum too
  // to guard against the silent version upconversion. See
  // https://github.com/facebook/rocksdb/blob/972f96b3fbae1a4675043bdf4279c9072ad69645/include/rocksdb/table.h#L198
  table_options.format_version = 0;
  table_options.checksum = rocksdb::kCRC32c;
  // table_options.filter_policy.reset(rocksdb::NewBloomFilterPolicy(10));

  rocksdb::Options options;
  options.table_factory.reset(rocksdb::NewBlockBasedTableFactory(table_options));

  // options.compression = rocksdb::kNoCompression;
  std::unique_ptr<rocksdb::SstFileWriter> tb(new rocksdb::SstFileWriter({}, options));
  status = tb->Open(outfile);
  if (!status.ok()) {
    std::cerr << "SstFileWriter::Open: " << status.ToString() << std::endl;
    return 1;
  }
  std::ifstream in(infile);
  std::string s;
  for (int i = 0; i < N && getline(in, s); i++) {
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

  std::cout << "wrote " << info.num_entries << " entries" << std::endl;
  return 0;
}

int main(int argc, char** argv) {
  int ret = write();
  if (ret != 0) {
    return ret;
  }
  return 0;
}
