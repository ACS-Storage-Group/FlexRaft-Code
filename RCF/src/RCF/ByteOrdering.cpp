
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

#include <RCF/ByteOrdering.hpp>
#include <RCF/Exception.hpp>
#include <RCF/Tools.hpp>

namespace RCF {

    enum ByteOrder
    {
        LittleEndian,
        BigEndian
    };

    // Compiler defines tell us what the machine byte ordering is.

#if     defined(__BYTE_ORDER) && __BYTE_ORDER == __BIG_ENDIAN                       \
    ||  defined(__BIG_ENDIAN__)                                                     \
    ||  defined(__ARMEB__)                                                          \
    ||  defined(__THUMBEB__)                                                        \
    ||  defined(__AARCH64EB__)                                                      \
    ||  defined(_MIBSEB) || defined(__MIBSEB) || defined(__MIBSEB__)                \
    ||  defined( __ppc__ ) || defined( __powerpc__ )                                \
    ||  defined(__sparc__) || defined(__sparc)                                      \
    ||  defined( __mips__ ) || defined(__mips64__)                                  \
    ||  defined(_M_PPC)

        const ByteOrder MachineByteOrder = BigEndian;

#elif   defined(__BYTE_ORDER) && __BYTE_ORDER == __LITTLE_ENDIAN                    \
    ||  defined(__LITTLE_ENDIAN__)                                                  \
    ||  defined(__ARMEL__)                                                          \
    ||  defined(__THUMBEL__)                                                        \
    ||  defined(__AARCH64EL__)                                                      \
    ||  defined(_MIPSEL) || defined(__MIPSEL) || defined(__MIPSEL__)                \
    ||  defined(_M_IX86) || defined(_M_X64) || defined(_M_IA64) || defined(_M_ARM)

        const ByteOrder MachineByteOrder = LittleEndian;

#else

    // If you get an error here, uncomment the appropriate line.
    //const ByteOrder MachineByteOrder = BigEndian;
    //const ByteOrder MachineByteOrder = LittleEndian;

#error Unknown byte order on this platform!

#endif

    const ByteOrder NetworkByteOrder = LittleEndian;

    void swapBytes(char *b1, char *b2)
    {
        char temp = *b1;
        *b1 = *b2;
        *b2 = temp;
    }

    void reverseByteOrder(void *buffer, int width, int count)
    {
        RCF_ASSERT(width > 0);
        RCF_ASSERT(count > 0);
        if (width == 1) return;

        char *chBuffer = static_cast<char *>(buffer);
        for (int i=0; i<count; i++)
        {
            for (int j=0;j<width/2;j++)
            {
                swapBytes(
                    chBuffer + i*width + j,
                    chBuffer + i*width + width - j - 1 );
            }
        }
    }

    void machineToNetworkOrder(void *buffer, int width, int count)
    {
        if RCF_CONSTEXPR(MachineByteOrder != NetworkByteOrder)
        {
            reverseByteOrder(buffer, width, count);
        }
    }

    void networkToMachineOrder(void *buffer, int width, int count)
    {
        if RCF_CONSTEXPR(MachineByteOrder != NetworkByteOrder)
        {
            reverseByteOrder(buffer, width, count);
        }
    }

    bool machineOrderEqualsNetworkOrder()
    {
        return MachineByteOrder == NetworkByteOrder;
    }

    bool isPlatformLittleEndian()
    {
        return MachineByteOrder == LittleEndian;
    }

} // namespace RCF
