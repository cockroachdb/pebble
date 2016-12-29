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

#include "leveldb/env.h"
#include "leveldb/filter_policy.h"
#include "leveldb/table.h"
#include "leveldb/table_builder.h"

const int N = 1000000;
const char* infile = "h.txt";
const char* outfile = "h.ldb";

int write() {
  leveldb::Status status;
  
  leveldb::WritableFile* wf;
  status = leveldb::Env::Default()->NewWritableFile(outfile, &wf);
  if (!status.ok()) {
    std::cerr << "Env::NewWritableFile: " << status.ToString() << std::endl;
    return 1;
  }

  leveldb::Options o;
  // o.compression = leveldb::kNoCompression;
  // o.filter_policy = leveldb::NewBloomFilterPolicy(10);
  leveldb::TableBuilder* tb = new leveldb::TableBuilder(o, wf);
  std::ifstream in(infile);
  std::string s;
  for (int i = 0; i < N && getline(in, s); i++) {
    std::string key(s, 8);
    std::string val(s, 0, 7);
    val = val.substr(1 + val.rfind(' '));
    tb->Add(key.c_str(), val.c_str());
  }

  status = tb->Finish();
  if (!status.ok()) {
    std::cerr << "TableBuilder::Finish: " << status.ToString() << std::endl;
    return 1;
  }

  status = wf->Close();
  if (!status.ok()) {
    std::cerr << "WritableFile::Close: " << status.ToString() << std::endl;
    return 1;
  }

  std::cout << "wrote " << tb->NumEntries() << " entries" << std::endl;
  delete tb;
  delete wf;
  return 0;
}

int read() {
  leveldb::Status status;

  leveldb::RandomAccessFile* raf;
  status = leveldb::Env::Default()->NewRandomAccessFile(outfile, &raf);
  if (!status.ok()) {
    std::cerr << "Env::NewRandomAccessFile: " << status.ToString() << std::endl;
    return 1;
  }

  uint64_t file_size;
  status = leveldb::Env::Default()->GetFileSize(outfile, &file_size);
  if (!status.ok()) {
    std::cerr << "Env::GetFileSize: " << status.ToString() << std::endl;
    return 1;
  }

  leveldb::Options o;
  leveldb::Table* t;
  status = leveldb::Table::Open(o, raf, file_size, &t);
  if (!status.ok()) {
    std::cerr << "Table::Open: " << status.ToString() << std::endl;
    return 1;
  }

  leveldb::ReadOptions ro;
  leveldb::Iterator* i = t->NewIterator(ro);
  uint64_t n = 0;
  for (i->SeekToFirst(); i->Valid(); i->Next()) {
    n++;
  }

  std::cout << "read  " << n << " entries" << std::endl;
  delete i;
  delete t;
  delete raf;
  return 0;
}

int main(int argc, char** argv) {
  int ret = write();
  if (ret != 0) {
    return ret;
  }
  return read();
}
