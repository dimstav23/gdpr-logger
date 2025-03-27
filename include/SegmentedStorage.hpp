#ifndef SEGMENTED_STORAGE_HPP
#define SEGMENTED_STORAGE_HPP

#include <filesystem>
#include <fstream>
#include <chrono>
#include <vector>
#include <mutex>
#include <string>
#include <sstream>
#include <iomanip>
#include <stdexcept>
#include <openssl/sha.h>

class SegmentedStorage
{
public:
    // Configuration for segment management
    struct SegmentConfig
    {
        size_t max_segment_size_bytes = 100 * 1024 * 1024;
        std::chrono::minutes max_segment_duration{60};
    };

    // Segment metadata structure
    struct SegmentMetadata
    {
        std::filesystem::path filename;
        std::chrono::system_clock::time_point created_at;
        size_t size_bytes = 0;
        std::string segment_hash;
    };

    // Constructor
    SegmentedStorage(
        const std::filesystem::path &base_directory,
        const SegmentConfig &config = SegmentConfig());

    // Write a log entry to the current segment
    void write_entry(const std::string &log_entry);

    // Retrieve completed segments for export/auditing
    std::vector<SegmentMetadata> get_completed_segments();

    // Export a specific segment
    void export_segment(
        const std::filesystem::path &segment_path,
        const std::filesystem::path &export_path);

private:
    // Current active segment file
    std::ofstream current_segment;

    // Segment file path generator
    std::filesystem::path base_path;

    // Configuration for segment management
    SegmentConfig config;

    // List of completed segments
    std::vector<SegmentMetadata> completed_segments;

    // Mutex to protect segment metadata
    std::mutex segments_mutex;

    // Timestamp of the current segment's creation
    std::chrono::system_clock::time_point segment_start_time;

    // Generate a unique segment filename
    std::filesystem::path generate_segment_filename();

    // Compute SHA-256 hash of the segment
    std::string compute_segment_hash(const std::filesystem::path &segment_path);

    // Rotate to a new segment if conditions are met
    void check_segment_rotation();

    // Rotate to a new segment file
    void rotate_segment();

    // Open a new segment file
    void open_new_segment();
};

#endif