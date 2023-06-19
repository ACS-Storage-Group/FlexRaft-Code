cmake_minimum_required(VERSION 3.8)

SET(CMAKE_CXX_STANDARD 17)

include_directories(include)
add_library(rcf STATIC src/RCF/RCF.cpp)
