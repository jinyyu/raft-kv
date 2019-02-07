#pragma once
#include <memory>

namespace kvd
{

class Transporter
{
public:
    virtual ~Transporter() = 0;

    virtual void start() = 0;

    virtual void stop() = 0;
};

typedef std::shared_ptr<Transporter> TransporterPtr;

}
