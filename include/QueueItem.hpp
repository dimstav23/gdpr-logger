#ifndef QUEUE_ITEM_HPP
#define QUEUE_ITEM_HPP

#include "LogEntry.hpp"
#include <optional>
#include <string>

struct QueueItem
{
    LogEntry entry;
    std::optional<std::string> targetFilename = std::nullopt;
};

#endif