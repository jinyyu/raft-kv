#pragma once
#include <string>

namespace kvd
{

class Status
{
public:
    // Create a success status.
    Status()
        : status_(nullptr)
    {}

    Status(const Status& s);

    Status& operator=(const Status& s);

    ~Status();

    static Status ok()
    { return Status(); }

    static Status not_found(const char* msg)
    { return Status(NotFound, msg); }

    static Status not_supported(const char* msg)
    { return Status(NotSupported, msg); }

    static Status invalid_argument(const char* msg)
    { return Status(InvalidArgument, msg); }

    static Status io_error(const char* msg)
    { return Status(IOError, msg); }

    bool is_ok() const
    { return status_ == nullptr; }

    bool is_not_found() const
    { return code() == Code::NotFound; }

    bool is_io_error() const
    { return code() == Code::IOError; }

    bool is_not_supported() const
    { return code() == NotSupported; }

    bool is_invalid_argument() const
    { return code() == InvalidArgument; }

    std::string to_string() const;

private:

    enum Code
    {
        OK = 0,
        NotFound = 1,
        NotSupported = 2,
        InvalidArgument = 3,
        IOError = 4,
    };

    inline static char* copy(const Status& s);

    Status(Code code, const char* msg);

    Code code() const
    {
        return status_ == nullptr ? Code::OK : static_cast<Code> (status_[4]);
    }

private:
    //state_[0..3] == length of message
    //state_[4]    == code
    //state_[5..]  == message
    char* status_;
};

}

