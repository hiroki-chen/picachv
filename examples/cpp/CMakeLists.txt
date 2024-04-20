cmake_minimum_required(VERSION 3.20)
project("ExampleCXX" CXX)

set(CMAKE_CXX_STANDARD 20)
find_package(Protobuf REQUIRED)

# Set the C++ standard
set(Protobuf_INCLUDE_DIRS ${CMAKE_SOURCE_DIR}/../../picachv-message/proto)
# Complie the ProtoBuf files
# Generate all files in that directory.
file(GLOB ProtoFiles "${Protobuf_INCLUDE_DIRS}/*.proto")
# Generate the C++ files
protobuf_generate_cpp(ProtoSources ProtoHeaders ${ProtoFiles})

# Include the generated files
INCLUDE_DIRECTORIES(${CMAKE_CURRENT_BINARY_DIR})
INCLUDE_DIRECTORIES(${Protobuf_INCLUDE_DIRS})

# Compile
add_executable(ExampleCXX main.cc ${ProtoSources} ${ProtoHeaders})
string(REGEX REPLACE "^[0-9]+\.([0-9]+\.[0-9]+)$" "\\1.0" proto_libver "${Protobuf_VERSION}")
if(proto_libver VERSION_GREATER_EQUAL "22")
    find_package(PkgConfig REQUIRED)
    pkg_check_modules(protobuf REQUIRED IMPORTED_TARGET protobuf=${proto_libver})
    target_link_libraries(ExampleCXX PRIVATE PkgConfig::protobuf picachv_core picachv_api)
else()
    target_link_libraries(ExampleCXX PRIVATE protobuf::libprotobuf picachv_core picachv_api)
endif()

target_link_directories(ExampleCXX PRIVATE ${CMAKE_CURRENT_SOURCE_DIR}/../../target/release)
