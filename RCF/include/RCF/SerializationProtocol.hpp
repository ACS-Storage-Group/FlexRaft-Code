
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

#ifndef INCLUDE_RCF_SERIALIZATIONPROTOCOL_HPP
#define INCLUDE_RCF_SERIALIZATIONPROTOCOL_HPP

#include <map>
#include <string>
#include <type_traits>

#include <RCF/ByteBuffer.hpp>
#include <RCF/MemStream.hpp>

#if RCF_FEATURE_PROTOBUF==1
#include <RCF/GoogleProtobufs.hpp>
#endif

// Serialization protocols

#include <RCF/Config.hpp>

#if RCF_FEATURE_SF==1
#include <RCF/SerializationProtocol_SF.hpp>
#endif

#if RCF_FEATURE_BOOST_SERIALIZATION==1
#include <RCF/SerializationProtocol_BS.hpp>
#endif

namespace RCF {

    RCF_EXPORT bool isSerializationProtocolSupported(int protocol);

    RCF_EXPORT std::string getSerializationProtocolName(int protocol);

    class MethodInvocationRequest;
    class MethodInvocationResponse;

    class RCF_EXPORT SerializationProtocolIn
    {
    public:
        SerializationProtocolIn();
        ~SerializationProtocolIn();

        MemIstream&     getIstream() { return mIs; }

        void            setSerializationProtocol(int protocol);
        int             getSerializationProtocol() const;

        void            reset(
                            const ByteBuffer &data, 
                            int protocol, 
                            int runtimeVersion, 
                            int archiveVersion,
                            bool enableSfPointerTracking);

        void            clearByteBuffer();
        void            clear();
        void            extractSlice(ByteBuffer &byteBuffer, std::size_t len);
        std::size_t     getArchiveLength();
        std::size_t     getRemainingArchiveLength();

        template<typename T>
        void read(const T *pt)
        {
            read(*pt);
        }

        template<typename T>
        void read(T &t)
        {
            try
            {
                switch (mProtocol)
                {
                case 1: mInProtocol1 >> t; break;
                case 2: mInProtocol2 >> t; break;
                case 3: mInProtocol3 >> t; break;
                case 4: mInProtocol4 >> t; break;
                case 5: mInProtocol5 >> t; break;

                default: RCF_ASSERT_ALWAYS("");
                }
            }
            catch(const RCF::Exception &e)
            {
                RCF_UNUSED_VARIABLE(e);
                throw;
            }
            catch(const std::exception &e)
            {
                RCF::Exception se( RcfError_Deserialization, 
                    typeid(t).name(), 
                    typeid(e).name(), 
                    e.what());

                RCF_THROW(se);
            }
        }

        int             getRuntimeVersion();

    private:

        void            bindProtocol();
        void            unbindProtocol();

        friend class ClientStub; // TODO
        friend class RcfSession; // TODO

        int                                     mProtocol;
        ByteBuffer                              mByteBuffer;
        MemIstream                              mIs;

        Protocol< Int<1> >::In     mInProtocol1;
        Protocol< Int<2> >::In     mInProtocol2;
        Protocol< Int<3> >::In     mInProtocol3;
        Protocol< Int<4> >::In     mInProtocol4;
        Protocol< Int<5> >::In     mInProtocol5;

        int                                     mRuntimeVersion;
        int                                     mArchiveVersion;
    };

    class RCF_EXPORT SerializationProtocolOut
    {
    public:
        SerializationProtocolOut();

        MemOstream& getMemOstream() { return *mOsPtr; }

        void    setSerializationProtocol(int protocol);
        int     getSerializationProtocol() const;
        void    clear();
        void    reset(
                    int protocol,
                    std::size_t margin,
                    ByteBuffer byteBuffer,
                    int runtimeVersion,
                    int archiveVersion,
                    bool enableSfPointerTracking);

        template<typename T>
        void    write(const T &t)
        {
            try
            {
                switch (mProtocol)
                {
                case 1: mOutProtocol1 << t; break;
                case 2: mOutProtocol2 << t; break;
                case 3: mOutProtocol3 << t; break;
                case 4: mOutProtocol4 << t; break;
                case 5: mOutProtocol5 << t; break;

                default: RCF_ASSERT_ALWAYS("");
                }
            }
            catch(const std::exception &e)
            {
                RCF::Exception se( RcfError_Serialization, 
                    typeid(t).name(), 
                    typeid(e).name(), 
                    e.what());

                RCF_THROW(se);
            }
        }

        void    insert(const ByteBuffer &byteBuffer);
        void    extractByteBuffers();
        void    extractByteBuffers(std::vector<ByteBuffer> &byteBuffers);

        int     getRuntimeVersion();

    private:

        void    bindProtocol();
        void    unbindProtocol();


        friend class ClientStub; // TODO
        friend class RcfSession; // TODO
        
        int                                                 mProtocol;
        std::size_t                                         mMargin;
        std::shared_ptr<MemOstream>                       mOsPtr;
        std::vector<std::pair<std::size_t, ByteBuffer> >    mByteBuffers;

        // these need to be below mOsPtr, for good order of destruction
        Protocol< Int<1> >::Out                mOutProtocol1;
        Protocol< Int<2> >::Out                mOutProtocol2;
        Protocol< Int<3> >::Out                mOutProtocol3;
        Protocol< Int<4> >::Out                mOutProtocol4;
        Protocol< Int<5> >::Out                mOutProtocol5;

        int                                                 mRuntimeVersion;
        int                                                 mArchiveVersion;
    };

    inline void serialize(
        SerializationProtocolOut &,
        const Void *)
    {
    }

    inline void serialize(
        SerializationProtocolOut &,
        const Void &)
    {
    }

    inline void deserialize(
        SerializationProtocolIn &,
        Void *)
    {
    }

    inline void deserialize(
        SerializationProtocolIn &,
        Void &)
    {
    }

    template<typename T>
    void serializeImpl(
        SerializationProtocolOut &out,
        const T &t,
        long int)
    {
        out.write(t);
    }

    template<typename T>
    void deserializeImpl(
        SerializationProtocolIn &in,
        T &t,
        long int)
    {
        in.read(t);
    }

#if RCF_FEATURE_PROTOBUF==1

    // Some compile-time gymnastics to detect Protobuf classes, so we don't 
    // pass them off to SF or Boost.Serialization.

    template<typename T>
    void serializeProtobufOrNot(
        SerializationProtocolOut &out,
        const T &t,
        RCF::FalseType *)
    {
        serializeImpl(out, t, 0);
    }

    template<typename T>
    void deserializeProtobufOrNot(
        SerializationProtocolIn &in,
        T &t,
        RCF::FalseType *)
    {
        deserializeImpl(in, t, 0);
    }

    template<typename T>
    void serializeProtobufOrNot(
        SerializationProtocolOut &out,
        const T & t,
        RCF::TrueType *)
    {
        MemOstream & os = out.getMemOstream();
        os.write("XXXX", 4);
        std::streamoff beginPos = os.tellp();

        if (!t.IsInitialized())
        {
            RCF_THROW(Exception(RcfError_ProtobufWriteInit, typeid(t).name()));
        }

        // Make room for the protobuf object.
        // TODO: Less obtuse way of reserving space.
        int byteSize = t.ByteSize();
        RCF_ASSERT(byteSize >= 0);
        for (int i=0; i<byteSize; ++i)
        {
            os.write("%", 1);
        }
        std::streamoff endPos = os.tellp();

        // Write the protobuf object straight into the underlying buffer.
        char * pch = os.str();
        bool ok = t.SerializeToArray(pch + beginPos, static_cast<int>(endPos - beginPos));
        if ( !ok )
        {
            Exception e(RcfError_ProtobufWrite, typeid(t).name());
            RCF_THROW(e);
        }

        // Write the prepended length field.
        std::uint32_t len = static_cast<std::uint32_t>(endPos - beginPos);
        char buffer[4] = {0};
        memcpy(buffer, &len, 4);
        RCF::machineToNetworkOrder(buffer, 4, 1);
        
        os.rdbuf()->pubseekoff(beginPos-4, std::ios::beg, std::ios::out);
        os.write(buffer, 4);
        os.rdbuf()->pubseekoff(endPos, std::ios::beg, std::ios::out);
    }

    template<typename T>
    void serializeProtobufOrNot(
        SerializationProtocolOut &out,
        const T * pt,
        RCF::TrueType *)
    {
        serializeProtobufOrNot(out, *pt, (RCF::TrueType *) NULL);
    }

    template<typename T>
    void serializeProtobufOrNot(
        SerializationProtocolOut &out,
        T * pt,
        RCF::TrueType *)
    {
        serializeProtobufOrNot(out, *pt, (RCF::TrueType *) NULL);
    }

    template<typename T>
    void deserializeProtobufOrNot(
        SerializationProtocolIn &in,
        T &t,
        RCF::TrueType *)
    {
        MemIstream & is = in.getIstream();

        char buffer[4];
        is.read(buffer, 4);
        std::uint32_t len = 0;
        memcpy( &len, buffer, 4);
        RCF::networkToMachineOrder(&len, 4, 1);

        ByteBuffer byteBuffer;
        in.extractSlice(byteBuffer, len);
        bool ok = t.ParseFromArray(byteBuffer.getPtr(), static_cast<int>(byteBuffer.getLength()));
        if ( !ok )
        {
            Exception e(RcfError_ProtobufRead, typeid(t).name());
            RCF_THROW(e);
        }
    }

    template<typename T>
    void deserializeProtobufOrNot(
        SerializationProtocolIn &in,
        T * & pt,
        RCF::TrueType *)
    {
        if (pt == NULL)
        {
            pt = new T();
        }

        deserializeProtobufOrNot(in, *pt, (RCF::TrueType *) NULL);
    }

    template<typename T>
    void serialize(
        SerializationProtocolOut &out,
        const T * pt)
    {
        typedef typename std::is_base_of<google::protobuf::Message, T>::type type;
        serializeProtobufOrNot(out, pt, (type *) NULL);
    }

    template<typename T>
    void serialize(
        SerializationProtocolOut &out,
        T * pt)
    {
        typedef typename std::is_base_of<google::protobuf::Message, T>::type type;
        serializeProtobufOrNot(out, pt, (type *) NULL);
    }

    template<typename T>
    void serialize(
        SerializationProtocolOut &out,
        const T & t)
    {
        typedef typename std::is_base_of<google::protobuf::Message, T>::type type;
        serializeProtobufOrNot(out, t, (type *) NULL);
    }

    template<typename T>
    void deserialize(
        SerializationProtocolIn &in,
        T * & pt)
    {
        typedef typename std::is_base_of<google::protobuf::Message, T>::type type;
        deserializeProtobufOrNot(in, pt, (type *) NULL);
    }

    template<typename T>
    void deserialize(
        SerializationProtocolIn &in,
        T & t)
    {
        typedef typename std::is_base_of<google::protobuf::Message, T>::type type;
        deserializeProtobufOrNot(in, t, (type *) NULL);
    }

#else

    // Whatever is passed to serialize() and deserialize(), is passed directly
    // on to the selected serialization framework. We have to be careful with
    // this, because Boost.Serialization is very picky about whether one 
    // serializes a pointer or a value.

    template<typename T>
    void serialize(
        SerializationProtocolOut &out,
        const T & t)
    {
        serializeImpl(out, t, 0);
    }

    template<typename T>
    void deserialize(
        SerializationProtocolIn &in,
        T & t)
    {
        deserializeImpl(in, t, 0);
    }

#endif

} // namespace RCF

#endif // ! INCLUDE_RCF_SERIALIZATIONPROTOCOL_HPP
