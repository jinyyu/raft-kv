#pragma once

#include <string.h>
#include <stdio.h>
#include <string>
#include <exception>

#ifdef DEBUG
#define LOG_DEBUG(format, ...) do { fprintf(stderr, "DEBUG [%s:%d] " format "\n", strrchr(__FILE__, '/') + 1, __LINE__, ##__VA_ARGS__); } while(0)
#else
#define LOG_DEBUG(format, ...)
#endif


#define LOG_INFO(format, ...) do { fprintf(stderr, "INFO [%s:%d] " format "\n", strrchr(__FILE__, '/') + 1, __LINE__, ##__VA_ARGS__); } while(0)
#define LOG_WARN(format, ...) do { fprintf(stderr, "WARN [%s:%d] " format "\n", strrchr(__FILE__, '/') + 1, __LINE__, ##__VA_ARGS__); } while(0)
#define LOG_ERROR(format, ...) do { fprintf(stderr, "ERROR [%s:%d] " format "\n", strrchr(__FILE__, '/') + 1, __LINE__, ##__VA_ARGS__); } while(0)
#define LOG_FATAL(format, ...) do \
        { \
             char buffer[1024];  \
             snprintf(buffer, sizeof(buffer), "FATAL [%s:%d] " format "\n", strrchr(__FILE__, '/') + 1, __LINE__, ##__VA_ARGS__); \
             throw std::runtime_error(buffer); \
        } while(0)

