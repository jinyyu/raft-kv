#pragma once

#include <string.h>
#include <stdio.h>
#include <string>

#ifdef DEBUG
#define LOG_DEBUG(format, ...) do { fprintf(stderr, "DEBUG [%s:%d] " format "\n", strrchr(__FILE__, '/') + 1, __LINE__, ##__VA_ARGS__); } while(0)
#else
#define LOG_DEBUG(format, ...)
#endif


#define LOG_INFO(format, ...) do { fprintf(stderr, "INFO [%s:%d] " format "\n", strrchr(__FILE__, '/') + 1, __LINE__, ##__VA_ARGS__); } while(0)
#define LOG_WARN(format, ...) do { fprintf(stderr, "WARN [%s:%d] " format "\n", strrchr(__FILE__, '/') + 1, __LINE__, ##__VA_ARGS__); } while(0)
#define LOG_ERROR(format, ...) do { fprintf(stderr, "ERROR [%s:%d] " format "\n", strrchr(__FILE__, '/') + 1, __LINE__, ##__VA_ARGS__); } while(0)