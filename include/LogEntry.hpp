#pragma once
#include <string>

struct LogEntry
{
    std::string action;
    std::string data;

    // You might want to add methods to serialize/deserialize etc.
};
