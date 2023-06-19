
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

#ifndef INCLUDE_SF_OBINARYSTREAM_HPP
#define INCLUDE_SF_OBINARYSTREAM_HPP

#include <SF/Stream.hpp>

namespace SF {

    /// Output stream using SF binary serialization.
    class OBinaryStream : public OStream
    {
    public:
        OBinaryStream() : OStream()
        {}

        OBinaryStream(RCF::MemOstream &os) : OStream(os)
        {}

        /// Constructs an OBinaryStream from a std::ostream. Serialized data will be written to the std::ostream.
        OBinaryStream(std::ostream &os) : OStream(os)
        {}

        I_Encoding &getEncoding()
        {
            return mEncoding;
        }

    private:
        EncodingBinaryPortable mEncoding;
    };

}

#endif // ! INCLUDE_SF_OBINARYSTREAM_HPP
