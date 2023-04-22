// General Libraries
#include <iostream>
#include <fstream>
#include <string>
#include <vector>
#include "csv.hpp"

// RocksDB Libraries
#include <rocksdb/db.h>
#include <rocksdb/options.h>


// Namespaces
using namespace std;
using ROCKSDB_NAMESPACE::DB;
using ROCKSDB_NAMESPACE::DBOptions;
using ROCKSDB_NAMESPACE::Options;
using ROCKSDB_NAMESPACE::Status;
using ROCKSDB_NAMESPACE::WriteBatch;
using ROCKSDB_NAMESPACE::WriteOptions;
using ROCKSDB_NAMESPACE::ReadOptions;
using ROCKSDB_NAMESPACE::Slice;


// Function to create a kvs
DB* create_kvs(const string& csv_file_path, const string& db_path) {
    DB* db;
    Options options;
    options.create_if_missing = true;
    rocksdb::Status status = rocksdb::DB::Open(options, db_path, &db);
    if (!status.ok()) {
        std::cerr << status.ToString() << std::endl;
        return db;
    }
    csv::CSVReader reader(csv_file_path);
    csv::CSVRow row;
    vector<string> header = reader.get_col_names();
    int columnNum = 0;
    for (csv::CSVRow& row : reader) { 
        columnNum = 0;
        for (csv::CSVField& field : row) {
            db->Put(rocksdb::WriteOptions(), row["id"].get<string>() + "_" + header[columnNum], field.get<string>());     
            columnNum++;
    	}
    }
    return db;
}


// Function to perform a MultiGet operation
vector<string> multi_get(DB* db, const vector<string>& keys) {
    vector<string> values;
    vector<Slice> sliceList;
    for (const string& k : keys) {
        sliceList.push_back(k);
    }
    db->MultiGet(ReadOptions(), sliceList, &values);
    // Only return the display_name of the subreddit(s)
    return values;
}

// Function to iterate over a range of keys and return the corresponding values
vector<string> iterate_over_range(DB* db, const string& start_key, const string& end_key) {
    string start = start_key + "_display_name";
    string end = end_key + "_display_name";
    vector<string> result;
    rocksdb::Iterator* iterator = db->NewIterator(ReadOptions());
    for (iterator->Seek(start); iterator->Valid() && iterator->key().ToString() < end; iterator->Next()) {
        if (iterator->key().ToString().find("display_name") != string::npos) {
            result.push_back(iterator->value().ToString());
        }
    }
    // Only return the display_name of the subreddit(s)
    return result;
}

// Function to delete a particular comment from the kvs
Status delete_key(DB* db, const string& key) {
    Status s;
    s = db->Delete(WriteOptions(), key);
    return s;
}
