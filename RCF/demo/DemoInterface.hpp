
#ifndef INCLUDE_DEMOINTERFACE_HPP
#define INCLUDE_DEMOINTERFACE_HPP

#include <string>
#include <vector>

#include <RCF/Idl.hpp>
#include <SF/vector.hpp>

RCF_BEGIN(I_DemoService, "I_DemoService")
    RCF_METHOD_V1(void, Reverse, std::vector<std::string> &);
RCF_END(I_DemoService);

#endif // ! INCLUDE_DEMOINTERFACE_HPP
