
//******************************************************************************
// RCF - Remote Call Framework
//
// Copyright (c) 2005 - 2020, Delta V Software. All rights reserved.
// http://www.deltavsoft.com
//
// RCF is distributed under dual licenses - closed source or GPL.
// Consult your particular license for conditions of use.
//
// If you have not purchased a commercial license, you are using RCF 
// under GPL terms.
//
// Version: 3.2
// Contact: support <at> deltavsoft.com 
//
//******************************************************************************

#ifndef INCLUDE_SF_IBINARYSTREAM_HPP
#define INCLUDE_SF_IBINARYSTREAM_HPP

#include <SF/Stream.hpp>

namespace SF {

    /// Input stream using SF binary serialization.
    class IBinaryStream : public IStream
    {
    public:
        IBinaryStream() : IStream()
        {}

        IBinaryStream(RCF::MemIstream &is) : IStream(is)
        {}

        /// Constructs an IBinaryStream from a std::istream. Serialized data will be read from the std::istream.
        IBinaryStream(std::istream &is) : IStream(is)
        {}

        I_Encoding &getEncoding()
        {
            return mEncoding;
        }

    private:
        EncodingBinaryPortable mEncoding;
    };

}

#endif // ! INCLUDE_SF_IBINARYSTREAM_HPP
