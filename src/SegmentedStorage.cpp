#include "SegmentedStorage.hpp"

SegmentedStorage::SegmentedStorage(
    const std::filesystem::path &base_directory,
    const SegmentConfig &config) : base_path(base_directory), config(config)
{
    // Ensure base directory exists
    std::filesystem::create_directories(base_path);

    // Open initial segment
    open_new_segment();
}

std::filesystem::path SegmentedStorage::generate_segment_filename()
{
    auto now = std::chrono::system_clock::now();
    auto timestamp = std::chrono::duration_cast<std::chrono::milliseconds>(
                         now.time_since_epoch())
                         .count();

    return base_path /
           ("segment_" + std::to_string(timestamp) + ".log");
}

std::string SegmentedStorage::compute_segment_hash(const std::filesystem::path &segment_path)
{
    std::ifstream file(segment_path, std::ios::binary);
    SHA256_CTX sha256;
    SHA256_Init(&sha256);

    std::vector<char> buffer(4096);
    while (file.read(buffer.data(), buffer.size()))
    {
        SHA256_Update(&sha256, buffer.data(), file.gcount());
    }

    if (file.gcount() > 0)
    {
        SHA256_Update(&sha256, buffer.data(), file.gcount());
    }

    unsigned char hash[SHA256_DIGEST_LENGTH];
    SHA256_Final(hash, &sha256);

    // Convert hash to hex string
    std::stringstream ss;
    for (int i = 0; i < SHA256_DIGEST_LENGTH; i++)
    {
        ss << std::hex << std::setw(2) << std::setfill('0')
           << static_cast<int>(hash[i]);
    }

    return ss.str();
}

void SegmentedStorage::check_segment_rotation()
{
    auto now = std::chrono::system_clock::now();
    bool should_rotate = false;

    // Check size threshold
    if (current_segment &&
        current_segment.tellp() >= config.max_segment_size_bytes)
    {
        should_rotate = true;
    }

    // Check time threshold
    if (now - segment_start_time >= config.max_segment_duration)
    {
        should_rotate = true;
    }

    if (should_rotate)
    {
        rotate_segment();
    }
}

void SegmentedStorage::rotate_segment()
{
    if (current_segment)
    {
        // Close current segment
        current_segment.close();

        // Compute and store segment hash
        auto last_segment_path = generate_segment_filename();
        std::string segment_hash = compute_segment_hash(last_segment_path);

        // Store segment metadata
        {
            std::lock_guard<std::mutex> lock(segments_mutex);
            completed_segments.push_back({last_segment_path,
                                          segment_start_time,
                                          static_cast<size_t>(current_segment.tellp()),
                                          segment_hash});
        }

        // Open new segment
        open_new_segment();
    }
}

void SegmentedStorage::open_new_segment()
{
    auto new_segment_path = generate_segment_filename();
    current_segment.open(new_segment_path, std::ios::binary | std::ios::app);

    if (!current_segment)
    {
        throw std::runtime_error("Failed to create new segment file");
    }

    segment_start_time = std::chrono::system_clock::now();
}

void SegmentedStorage::write_entry(const std::string &log_entry)
{
    // Ensure thread-safety for writing
    std::lock_guard<std::mutex> lock(segments_mutex);

    // Write entry
    current_segment << log_entry << std::endl;
    current_segment.flush();

    // Check if segment needs rotation
    check_segment_rotation();
}

std::vector<SegmentedStorage::SegmentMetadata>
SegmentedStorage::get_completed_segments()
{
    std::lock_guard<std::mutex> lock(segments_mutex);
    return completed_segments;
}

void SegmentedStorage::export_segment(
    const std::filesystem::path &segment_path,
    const std::filesystem::path &export_path)
{
    // In a real implementation, this would include decryption,
    // hash verification, etc.
    std::filesystem::copy(segment_path, export_path);
}