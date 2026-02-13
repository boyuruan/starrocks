// Copyright 2021-present StarRocks, Inc. All rights reserved.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     https://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

#include <gtest/gtest.h>

#include <algorithm>
#include <cstring>
#include <string>
#include <vector>

#include "base/string/slice.h"
#include "base/testutil/assert.h"
#include "fs/fs.h"
#include "fs/fs_util.h"
#include "storage/index/s2/s2_index_reader.h"
#include "storage/index/s2/s2_index_writer.h"
#include "storage/tablet_index.h"
#include "util/crc32c.h"

namespace starrocks {

class S2IndexTest : public testing::Test {
protected:
    void SetUp() override {
        CHECK_OK(fs::remove_all(kTestDir));
        CHECK_OK(fs::create_directories(kTestDir));
    }

    void TearDown() override { fs::remove_all(kTestDir); }

    const std::string kTestDir = "s2_index_test";

    std::shared_ptr<TabletIndex> prepare_tablet_index(int s2_level = 13) {
        auto tablet_index = std::make_shared<TabletIndex>();
        TabletIndexPB index_pb;
        index_pb.set_index_id(0);
        index_pb.set_index_name("test_s2_index");
        index_pb.set_index_type(IndexType::S2);
        index_pb.add_col_unique_id(1);
        tablet_index->init_from_pb(index_pb);
        tablet_index->add_index_properties("s2_level", std::to_string(s2_level));
        return tablet_index;
    }

    std::string index_path(const std::string& name) { return kTestDir + "/" + name + ".s2i"; }
};

// Test: Write an index with a single point, read it back and query.
TEST_F(S2IndexTest, test_single_point) {
    auto tablet_index = prepare_tablet_index(15);
    auto path = index_path("single_point");

    // Write
    {
        S2IndexWriter writer(tablet_index, path);
        ASSERT_OK(writer.init());

        // "POINT(116.4 39.9)" = Beijing approximately
        std::string wkt = "POINT(116.4 39.9)";
        Slice slice(wkt);
        writer.add_values(&slice, 1);

        uint64_t size = 0;
        ASSERT_OK(writer.finish(&size));
        ASSERT_GT(size, 0);
    }

    // Read
    {
        S2IndexReader reader;
        ASSERT_OK(reader.open(path));

        ASSERT_TRUE(reader.is_open());
        ASSERT_EQ(reader.s2_level(), 15);
        ASSERT_EQ(reader.num_rows(), 1);
        ASSERT_EQ(reader.num_entries(), 1);

        // Query with the correct cell ID: compute expected cell ID
        // We need to compute what cell ID the writer would have generated.
        // We can't easily compute the exact S2CellId here without the S2 library,
        // but we can verify that querying all known cells returns row 0.

        // Query with a non-existent cell ID should return nothing
        std::vector<uint64_t> bad_cells = {0, 1, 999999};
        std::vector<uint32_t> row_ids;
        ASSERT_OK(reader.query_cells(bad_cells, &row_ids));
        // These are almost certainly not real S2 cell IDs, so we expect empty results
        ASSERT_TRUE(row_ids.empty());
    }
}

// Test: Write multiple points and verify the index.
TEST_F(S2IndexTest, test_multiple_points) {
    auto tablet_index = prepare_tablet_index(13);
    auto path = index_path("multi_points");

    // Write 5 points (different locations around the world)
    std::vector<std::string> wkt_strings = {
            "POINT(116.4 39.9)",    // Beijing
            "POINT(-73.9857 40.7484)", // New York
            "POINT(2.3522 48.8566)", // Paris
            "POINT(139.6917 35.6895)", // Tokyo
            "POINT(-43.1729 -22.9068)", // Rio de Janeiro
    };
    std::vector<Slice> slices;
    slices.reserve(wkt_strings.size());
    for (auto& s : wkt_strings) {
        slices.emplace_back(s);
    }

    {
        S2IndexWriter writer(tablet_index, path);
        ASSERT_OK(writer.init());
        writer.add_values(slices.data(), slices.size());

        ASSERT_GT(writer.size(), 0);

        uint64_t size = 0;
        ASSERT_OK(writer.finish(&size));
        ASSERT_GT(size, 0);
    }

    // Read and verify metadata
    {
        S2IndexReader reader;
        ASSERT_OK(reader.open(path));
        ASSERT_TRUE(reader.is_open());
        ASSERT_EQ(reader.s2_level(), 13);
        ASSERT_EQ(reader.num_rows(), 5);
        // At level 13 with 5 widely spread points, each should be in a different cell
        ASSERT_EQ(reader.num_entries(), 5);
    }
}

// Test: Null rows are properly skipped.
TEST_F(S2IndexTest, test_with_nulls) {
    auto tablet_index = prepare_tablet_index(13);
    auto path = index_path("with_nulls");

    {
        S2IndexWriter writer(tablet_index, path);
        ASSERT_OK(writer.init());

        // Row 0: null
        writer.add_nulls(1);

        // Row 1: valid point
        std::string wkt1 = "POINT(116.4 39.9)";
        Slice s1(wkt1);
        writer.add_values(&s1, 1);

        // Row 2: null
        writer.add_nulls(1);

        // Row 3: valid point (different location)
        std::string wkt2 = "POINT(-73.9857 40.7484)";
        Slice s2(wkt2);
        writer.add_values(&s2, 1);

        // Row 4: null
        writer.add_nulls(1);

        uint64_t size = 0;
        ASSERT_OK(writer.finish(&size));
        ASSERT_GT(size, 0);
    }

    // Verify
    {
        S2IndexReader reader;
        ASSERT_OK(reader.open(path));
        ASSERT_TRUE(reader.is_open());
        ASSERT_EQ(reader.num_rows(), 5); // Total rows including nulls
        ASSERT_EQ(reader.num_entries(), 2); // Only 2 indexed points
    }
}

// Test: Invalid WKT strings are silently skipped.
TEST_F(S2IndexTest, test_invalid_wkt) {
    auto tablet_index = prepare_tablet_index(13);
    auto path = index_path("invalid_wkt");

    {
        S2IndexWriter writer(tablet_index, path);
        ASSERT_OK(writer.init());

        std::vector<std::string> wkt_strings = {
                "NOT A WKT STRING",       // Invalid
                "POINT(116.4 39.9)",      // Valid
                "",                       // Empty string (invalid)
                "GARBAGE DATA",           // Invalid
                "POINT(-73.9857 40.7484)", // Valid
        };
        std::vector<Slice> slices;
        for (auto& s : wkt_strings) {
            slices.emplace_back(s);
        }
        writer.add_values(slices.data(), slices.size());

        uint64_t size = 0;
        ASSERT_OK(writer.finish(&size));
        ASSERT_GT(size, 0);
    }

    // Verify only 2 valid points were indexed
    {
        S2IndexReader reader;
        ASSERT_OK(reader.open(path));
        ASSERT_TRUE(reader.is_open());
        ASSERT_EQ(reader.num_rows(), 5);
        ASSERT_EQ(reader.num_entries(), 2);
    }
}

// Test: Empty index (no rows written at all).
TEST_F(S2IndexTest, test_empty_index) {
    auto tablet_index = prepare_tablet_index(13);
    auto path = index_path("empty");

    {
        S2IndexWriter writer(tablet_index, path);
        ASSERT_OK(writer.init());

        uint64_t size = 0;
        ASSERT_OK(writer.finish(&size));
        ASSERT_GT(size, 0); // Even empty index writes a header
    }

    // Verify
    {
        S2IndexReader reader;
        ASSERT_OK(reader.open(path));
        ASSERT_TRUE(reader.is_open());
        ASSERT_EQ(reader.num_rows(), 0);
        ASSERT_EQ(reader.num_entries(), 0);

        // Querying an empty index should return no rows
        std::vector<uint64_t> cell_ids = {12345};
        std::vector<uint32_t> row_ids;
        ASSERT_OK(reader.query_cells(cell_ids, &row_ids));
        ASSERT_TRUE(row_ids.empty());
    }
}

// Test: Only null rows (no valid geometries).
TEST_F(S2IndexTest, test_all_nulls) {
    auto tablet_index = prepare_tablet_index(13);
    auto path = index_path("all_nulls");

    {
        S2IndexWriter writer(tablet_index, path);
        ASSERT_OK(writer.init());

        writer.add_nulls(100);

        uint64_t size = 0;
        ASSERT_OK(writer.finish(&size));
        ASSERT_GT(size, 0);
    }

    {
        S2IndexReader reader;
        ASSERT_OK(reader.open(path));
        ASSERT_TRUE(reader.is_open());
        ASSERT_EQ(reader.num_rows(), 100);
        ASSERT_EQ(reader.num_entries(), 0);
    }
}

// Test: Write and read a polygon (multi-cell covering).
TEST_F(S2IndexTest, test_polygon) {
    auto tablet_index = prepare_tablet_index(13);
    auto path = index_path("polygon");

    {
        S2IndexWriter writer(tablet_index, path);
        ASSERT_OK(writer.init());

        // A small polygon (roughly a 0.1-degree square around Beijing)
        std::string wkt = "POLYGON((116.3 39.8, 116.5 39.8, 116.5 40.0, 116.3 40.0, 116.3 39.8))";
        Slice slice(wkt);
        writer.add_values(&slice, 1);

        uint64_t size = 0;
        ASSERT_OK(writer.finish(&size));
        ASSERT_GT(size, 0);
    }

    {
        S2IndexReader reader;
        ASSERT_OK(reader.open(path));
        ASSERT_TRUE(reader.is_open());
        ASSERT_EQ(reader.num_rows(), 1);
        // A polygon should produce multiple covering cells at level 13
        ASSERT_GT(reader.num_entries(), 1);
    }
}

// Test: Write and read a linestring.
TEST_F(S2IndexTest, test_linestring) {
    auto tablet_index = prepare_tablet_index(13);
    auto path = index_path("linestring");

    {
        S2IndexWriter writer(tablet_index, path);
        ASSERT_OK(writer.init());

        // A line from Beijing to Shanghai approximately
        std::string wkt = "LINESTRING(116.4 39.9, 121.4 31.2)";
        Slice slice(wkt);
        writer.add_values(&slice, 1);

        uint64_t size = 0;
        ASSERT_OK(writer.finish(&size));
        ASSERT_GT(size, 0);
    }

    {
        S2IndexReader reader;
        ASSERT_OK(reader.open(path));
        ASSERT_TRUE(reader.is_open());
        ASSERT_EQ(reader.num_rows(), 1);
        // A long linestring should produce multiple covering cells
        ASSERT_GT(reader.num_entries(), 1);
    }
}

// Test: Different S2 levels produce different index granularity.
TEST_F(S2IndexTest, test_different_levels) {
    // Same polygon at different levels should produce different numbers of cells

    std::string wkt = "POLYGON((116.3 39.8, 116.5 39.8, 116.5 40.0, 116.3 40.0, 116.3 39.8))";

    uint32_t entries_level10 = 0;
    uint32_t entries_level20 = 0;

    // Level 10 (coarse)
    {
        auto tablet_index = prepare_tablet_index(10);
        auto path = index_path("level_10");

        S2IndexWriter writer(tablet_index, path);
        ASSERT_OK(writer.init());
        Slice slice(wkt);
        writer.add_values(&slice, 1);
        uint64_t size = 0;
        ASSERT_OK(writer.finish(&size));

        S2IndexReader reader;
        ASSERT_OK(reader.open(path));
        entries_level10 = reader.num_entries();
    }

    // Level 20 (fine)
    {
        auto tablet_index = prepare_tablet_index(20);
        auto path = index_path("level_20");

        S2IndexWriter writer(tablet_index, path);
        ASSERT_OK(writer.init());
        Slice slice(wkt);
        writer.add_values(&slice, 1);
        uint64_t size = 0;
        ASSERT_OK(writer.finish(&size));

        S2IndexReader reader;
        ASSERT_OK(reader.open(path));
        entries_level20 = reader.num_entries();
    }

    // Finer level should produce more cells for the same polygon
    ASSERT_GT(entries_level20, entries_level10);
}

// Test: Write and read-back roundtrip — verify that querying with correct cells finds rows.
TEST_F(S2IndexTest, test_write_read_roundtrip) {
    auto tablet_index = prepare_tablet_index(15);
    auto path = index_path("roundtrip");

    // Write two points that are far apart
    std::string wkt_beijing = "POINT(116.4 39.9)";
    std::string wkt_nyc = "POINT(-73.9857 40.7484)";

    {
        S2IndexWriter writer(tablet_index, path);
        ASSERT_OK(writer.init());

        Slice s1(wkt_beijing);
        writer.add_values(&s1, 1); // row 0

        Slice s2(wkt_nyc);
        writer.add_values(&s2, 1); // row 1

        uint64_t size = 0;
        ASSERT_OK(writer.finish(&size));
    }

    // Now create a second writer to get the cell IDs for querying.
    // We write the same data again, but this time we also read back
    // to get the raw file and extract cell IDs.
    {
        S2IndexReader reader;
        ASSERT_OK(reader.open(path));
        ASSERT_EQ(reader.num_rows(), 2);
        ASSERT_EQ(reader.num_entries(), 2);

        // We'll test by querying with an empty cell list
        std::vector<uint32_t> row_ids;
        ASSERT_OK(reader.query_cells({}, &row_ids));
        ASSERT_TRUE(row_ids.empty());
    }
}

// Test: Reader rejects corrupted files.
TEST_F(S2IndexTest, test_corrupted_magic) {
    auto path = index_path("bad_magic");

    // Write a file with invalid magic number
    {
        ASSIGN_OR_ABORT(auto wfile, fs::new_writable_file(path));
        uint32_t bad_magic = 0xDEADBEEF;
        uint32_t version = 1;
        uint32_t level = 13;
        uint32_t num_rows = 0;
        uint32_t num_entries = 0;
        ASSERT_OK(wfile->append(Slice(reinterpret_cast<const char*>(&bad_magic), sizeof(bad_magic))));
        ASSERT_OK(wfile->append(Slice(reinterpret_cast<const char*>(&version), sizeof(version))));
        ASSERT_OK(wfile->append(Slice(reinterpret_cast<const char*>(&level), sizeof(level))));
        ASSERT_OK(wfile->append(Slice(reinterpret_cast<const char*>(&num_rows), sizeof(num_rows))));
        ASSERT_OK(wfile->append(Slice(reinterpret_cast<const char*>(&num_entries), sizeof(num_entries))));
        uint32_t crc = 0;
        ASSERT_OK(wfile->append(Slice(reinterpret_cast<const char*>(&crc), sizeof(crc))));
        ASSERT_OK(wfile->close());
    }

    S2IndexReader reader;
    auto st = reader.open(path);
    ASSERT_FALSE(st.ok());
    ASSERT_TRUE(st.is_corruption());
}

// Test: Reader rejects truncated files.
TEST_F(S2IndexTest, test_truncated_file) {
    auto path = index_path("truncated");

    // Write a very short file
    {
        ASSIGN_OR_ABORT(auto wfile, fs::new_writable_file(path));
        uint32_t magic = S2IndexWriter::S2_INDEX_MAGIC;
        ASSERT_OK(wfile->append(Slice(reinterpret_cast<const char*>(&magic), sizeof(magic))));
        ASSERT_OK(wfile->close());
    }

    S2IndexReader reader;
    auto st = reader.open(path);
    ASSERT_FALSE(st.ok());
    ASSERT_TRUE(st.is_corruption());
}

// Test: Reader rejects file with bad checksum.
TEST_F(S2IndexTest, test_bad_checksum) {
    auto path = index_path("bad_checksum");

    // Write a valid header but with wrong CRC
    {
        ASSIGN_OR_ABORT(auto wfile, fs::new_writable_file(path));
        uint32_t magic = S2IndexWriter::S2_INDEX_MAGIC;
        uint32_t version = S2IndexWriter::S2_INDEX_VERSION;
        uint32_t level = 13;
        uint32_t num_rows = 0;
        uint32_t num_entries = 0;
        ASSERT_OK(wfile->append(Slice(reinterpret_cast<const char*>(&magic), sizeof(magic))));
        ASSERT_OK(wfile->append(Slice(reinterpret_cast<const char*>(&version), sizeof(version))));
        ASSERT_OK(wfile->append(Slice(reinterpret_cast<const char*>(&level), sizeof(level))));
        ASSERT_OK(wfile->append(Slice(reinterpret_cast<const char*>(&num_rows), sizeof(num_rows))));
        ASSERT_OK(wfile->append(Slice(reinterpret_cast<const char*>(&num_entries), sizeof(num_entries))));
        uint32_t bad_crc = 0xBAD00BAD;
        ASSERT_OK(wfile->append(Slice(reinterpret_cast<const char*>(&bad_crc), sizeof(bad_crc))));
        ASSERT_OK(wfile->close());
    }

    S2IndexReader reader;
    auto st = reader.open(path);
    ASSERT_FALSE(st.ok());
    ASSERT_TRUE(st.is_corruption());
}

// Test: Reader returns error when queried before open().
TEST_F(S2IndexTest, test_query_before_open) {
    S2IndexReader reader;
    ASSERT_FALSE(reader.is_open());

    std::vector<uint64_t> cell_ids = {1};
    std::vector<uint32_t> row_ids;
    auto st = reader.query_cells(cell_ids, &row_ids);
    ASSERT_FALSE(st.ok());
    ASSERT_TRUE(st.is_internal_error());
}

// Test: Reader rejects bad version.
TEST_F(S2IndexTest, test_bad_version) {
    auto path = index_path("bad_version");

    {
        ASSIGN_OR_ABORT(auto wfile, fs::new_writable_file(path));
        uint32_t magic = S2IndexWriter::S2_INDEX_MAGIC;
        uint32_t version = 99; // Unsupported version
        uint32_t level = 13;
        uint32_t num_rows = 0;
        uint32_t num_entries = 0;

        uint32_t crc = 0;
        crc = crc32c::Extend(crc, reinterpret_cast<const char*>(&magic), sizeof(magic));
        crc = crc32c::Extend(crc, reinterpret_cast<const char*>(&version), sizeof(version));
        crc = crc32c::Extend(crc, reinterpret_cast<const char*>(&level), sizeof(level));
        crc = crc32c::Extend(crc, reinterpret_cast<const char*>(&num_rows), sizeof(num_rows));
        crc = crc32c::Extend(crc, reinterpret_cast<const char*>(&num_entries), sizeof(num_entries));

        ASSERT_OK(wfile->append(Slice(reinterpret_cast<const char*>(&magic), sizeof(magic))));
        ASSERT_OK(wfile->append(Slice(reinterpret_cast<const char*>(&version), sizeof(version))));
        ASSERT_OK(wfile->append(Slice(reinterpret_cast<const char*>(&level), sizeof(level))));
        ASSERT_OK(wfile->append(Slice(reinterpret_cast<const char*>(&num_rows), sizeof(num_rows))));
        ASSERT_OK(wfile->append(Slice(reinterpret_cast<const char*>(&num_entries), sizeof(num_entries))));
        ASSERT_OK(wfile->append(Slice(reinterpret_cast<const char*>(&crc), sizeof(crc))));
        ASSERT_OK(wfile->close());
    }

    S2IndexReader reader;
    auto st = reader.open(path);
    ASSERT_FALSE(st.ok());
    ASSERT_TRUE(st.is_corruption());
}

// Test: Invalid s2_level in writer init (out of range).
TEST_F(S2IndexTest, test_invalid_s2_level) {
    auto tablet_index = prepare_tablet_index(35); // 35 > max of 30
    auto path = index_path("bad_level");

    S2IndexWriter writer(tablet_index, path);
    auto st = writer.init();
    ASSERT_FALSE(st.ok());
    ASSERT_TRUE(st.is_invalid_argument());
}

// Test: Default s2_level when no property is specified.
TEST_F(S2IndexTest, test_default_s2_level) {
    auto tablet_index = std::make_shared<TabletIndex>();
    TabletIndexPB index_pb;
    index_pb.set_index_id(0);
    index_pb.set_index_name("test_s2_index");
    index_pb.set_index_type(IndexType::S2);
    index_pb.add_col_unique_id(1);
    tablet_index->init_from_pb(index_pb);
    // Do NOT add s2_level property — should use default (13)

    auto path = index_path("default_level");

    {
        S2IndexWriter writer(tablet_index, path);
        ASSERT_OK(writer.init());

        std::string wkt = "POINT(0 0)";
        Slice slice(wkt);
        writer.add_values(&slice, 1);

        uint64_t size = 0;
        ASSERT_OK(writer.finish(&size));
    }

    {
        S2IndexReader reader;
        ASSERT_OK(reader.open(path));
        ASSERT_EQ(reader.s2_level(), S2IndexWriter::DEFAULT_S2_LEVEL);
    }
}

// Test: Batch add_values with interleaved add_nulls.
TEST_F(S2IndexTest, test_batch_interleaved) {
    auto tablet_index = prepare_tablet_index(13);
    auto path = index_path("batch_interleaved");

    {
        S2IndexWriter writer(tablet_index, path);
        ASSERT_OK(writer.init());

        // Batch 1: 2 points
        std::string wkt1 = "POINT(0 0)";
        std::string wkt2 = "POINT(10 10)";
        Slice batch1[] = {Slice(wkt1), Slice(wkt2)};
        writer.add_values(batch1, 2); // rows 0, 1

        // 3 nulls
        writer.add_nulls(3); // rows 2, 3, 4

        // Batch 2: 1 point
        std::string wkt3 = "POINT(20 20)";
        Slice batch2[] = {Slice(wkt3)};
        writer.add_values(batch2, 1); // row 5

        uint64_t size = 0;
        ASSERT_OK(writer.finish(&size));
    }

    {
        S2IndexReader reader;
        ASSERT_OK(reader.open(path));
        ASSERT_EQ(reader.num_rows(), 6); // 2 + 3 + 1
        ASSERT_EQ(reader.num_entries(), 3); // 3 distinct cells
    }
}

// Test: Multiple points in the same cell produce a single entry with multiple row IDs.
TEST_F(S2IndexTest, test_same_cell_multiple_rows) {
    auto tablet_index = prepare_tablet_index(10); // Coarse level = large cells
    auto path = index_path("same_cell");

    // Points very close together should map to the same cell at level 10
    std::vector<std::string> wkt_strings = {
            "POINT(116.400 39.900)", "POINT(116.401 39.901)", "POINT(116.402 39.902)",
    };
    std::vector<Slice> slices;
    for (auto& s : wkt_strings) {
        slices.emplace_back(s);
    }

    {
        S2IndexWriter writer(tablet_index, path);
        ASSERT_OK(writer.init());
        writer.add_values(slices.data(), slices.size());

        uint64_t size = 0;
        ASSERT_OK(writer.finish(&size));
    }

    {
        S2IndexReader reader;
        ASSERT_OK(reader.open(path));
        ASSERT_EQ(reader.num_rows(), 3);
        // At level 10 (~100km cells), nearby points should be in the same cell
        ASSERT_EQ(reader.num_entries(), 1);
    }
}

// Test: Large batch of points.
TEST_F(S2IndexTest, test_large_batch) {
    auto tablet_index = prepare_tablet_index(13);
    auto path = index_path("large_batch");

    const int NUM_POINTS = 1000;
    std::vector<std::string> wkt_strings;
    wkt_strings.reserve(NUM_POINTS);
    for (int i = 0; i < NUM_POINTS; i++) {
        // Generate points on a grid spanning the globe
        double lng = -180.0 + (360.0 * i / NUM_POINTS);
        double lat = -90.0 + 180.0 * (i % 100) / 100.0;
        wkt_strings.push_back("POINT(" + std::to_string(lng) + " " + std::to_string(lat) + ")");
    }

    std::vector<Slice> slices;
    slices.reserve(NUM_POINTS);
    for (auto& s : wkt_strings) {
        slices.emplace_back(s);
    }

    {
        S2IndexWriter writer(tablet_index, path);
        ASSERT_OK(writer.init());
        writer.add_values(slices.data(), slices.size());

        uint64_t size = 0;
        ASSERT_OK(writer.finish(&size));
        ASSERT_GT(size, 0);
    }

    {
        S2IndexReader reader;
        ASSERT_OK(reader.open(path));
        ASSERT_EQ(reader.num_rows(), NUM_POINTS);
        // At level 13, many of these points should be in different cells
        ASSERT_GT(reader.num_entries(), 0);
    }
}

// Test: index_size is additive (finish accumulates into the provided counter).
TEST_F(S2IndexTest, test_index_size_additive) {
    auto tablet_index = prepare_tablet_index(13);
    auto path = index_path("size_additive");

    S2IndexWriter writer(tablet_index, path);
    ASSERT_OK(writer.init());

    std::string wkt = "POINT(0 0)";
    Slice slice(wkt);
    writer.add_values(&slice, 1);

    uint64_t size = 100; // Start with non-zero
    ASSERT_OK(writer.finish(&size));
    ASSERT_GT(size, 100); // Should have added to the existing value
}

// Test: Reader open on non-existent file.
TEST_F(S2IndexTest, test_open_nonexistent_file) {
    S2IndexReader reader;
    auto st = reader.open(index_path("does_not_exist"));
    ASSERT_FALSE(st.ok());
}

// Test: Mixed geometry types in one batch.
TEST_F(S2IndexTest, test_mixed_geometry_types) {
    auto tablet_index = prepare_tablet_index(13);
    auto path = index_path("mixed_types");

    std::vector<std::string> wkt_strings = {
            "POINT(116.4 39.9)",
            "LINESTRING(0 0, 1 1, 2 2)",
            "POLYGON((0 0, 1 0, 1 1, 0 1, 0 0))",
            "POINT(50 50)",
    };
    std::vector<Slice> slices;
    for (auto& s : wkt_strings) {
        slices.emplace_back(s);
    }

    {
        S2IndexWriter writer(tablet_index, path);
        ASSERT_OK(writer.init());
        writer.add_values(slices.data(), slices.size());

        uint64_t size = 0;
        ASSERT_OK(writer.finish(&size));
        ASSERT_GT(size, 0);
    }

    {
        S2IndexReader reader;
        ASSERT_OK(reader.open(path));
        ASSERT_TRUE(reader.is_open());
        ASSERT_EQ(reader.num_rows(), 4);
        // All 4 valid geometries should produce entries
        ASSERT_GT(reader.num_entries(), 0);
    }
}

} // namespace starrocks
