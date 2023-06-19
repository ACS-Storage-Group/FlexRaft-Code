
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

#ifndef INCLUDE_RCF_PROTOCOL_PROTOCOL_HPP
#define INCLUDE_RCF_PROTOCOL_PROTOCOL_HPP

#include <functional>

#include <RCF/Config.hpp>
#include <RCF/Enums.hpp>
#include <RCF/ErrorMsg.hpp>
#include <RCF/MemStream.hpp>
#include <RCF/Tools.hpp>

namespace RCF {

    class SerializationProtocolIn;
    class SerializationProtocolOut;

    extern const SerializationProtocol DefaultSerializationProtocol;

    template<typename N>
    class Protocol
    {
    public:

        static std::string getName()
        {
            return "";
        }

        class In
        {
        public:
            void bind(
                MemIstream & is,
                std::size_t archiveSize, 
                int runtimeVersion, 
                int archiveVersion,
                SerializationProtocolIn & spIn)
            {
                RCF_UNUSED_VARIABLE(is);
                RCF_UNUSED_VARIABLE(archiveSize);
                RCF_UNUSED_VARIABLE(runtimeVersion);
                RCF_UNUSED_VARIABLE(archiveVersion);
                RCF_UNUSED_VARIABLE(spIn);
            }

            void unbind()
            {}
         
            template<typename T>
            In &operator>>(T &t)
            {
                RCF_UNUSED_VARIABLE(t);
                RCF_THROW(RCF::Exception(
                    RcfError_UnknownSerializationProtocol, 
                    N::value));
                return *this;
            }
        };

        class Out
        {
        public:
            void bind(
                MemOstream & os,
                int runtimeVersion, 
                int archiveVersion,
                SerializationProtocolOut & spOut)
            {
                RCF_UNUSED_VARIABLE(os);
                RCF_UNUSED_VARIABLE(runtimeVersion);
                RCF_UNUSED_VARIABLE(archiveVersion);
                RCF_UNUSED_VARIABLE(spOut);
            }

            void unbind()
            {}

            template<typename T>
            Out &operator<<(const T &t)
            {
                RCF_UNUSED_VARIABLE(t);
                RCF_THROW(RCF::Exception(
                    RcfError_UnknownSerializationProtocol, 
                    N::value));
                return *this;
            }
        };
    };


    template<typename IS, typename OS>
    class ProtocolImpl_SF
    {
    public:
        class In
        {
        public:
            void bind(
                MemIstream & is,
                std::size_t archiveSize, 
                int runtimeVersion, 
                int archiveVersion,
                SerializationProtocolIn & spIn)
            {
                mAr.clearState();
                mAr.setIs(is, archiveSize, runtimeVersion, archiveVersion);
                mAr.setRemoteCallContext(&spIn);
            }

            void unbind()
            {}
           
            template<typename T> In &operator>>(T &t)
            {
                mAr >> t;
                return *this;
            }

            IS & getIStream()
            {
                return mAr;
            }
       
        private:
            IS mAr;

        };

        class Out
        {
        public:
            void bind(
                RCF::MemOstream &os, 
                int runtimeVersion, 
                int archiveVersion,
                SerializationProtocolOut & spOut)
            {
                mAr.clearState();
                mAr.setOs(os, runtimeVersion, archiveVersion);
                mAr.setRemoteCallContext(&spOut);
                mAr.suppressArchiveMetadata();
            }

            void unbind()
            {}
           
            template<typename T> Out &operator<<(const T &t)
            {
                mAr << t;
                return *this;
            }

            OS & getOStream() { return mAr; }

        private:
            OS mAr;

        };
    };
   
    template<typename IS, typename OS>
    class ProtocolImpl_BSer
    {
    public:
        class In
        {
        public:
            void bind(
                MemIstream & is,
                std::size_t archiveSize, 
                int runtimeVersion, 
                int archiveVersion,
                SerializationProtocolIn & spIn)
            {
                RCF_UNUSED_VARIABLE(archiveSize);
                RCF_UNUSED_VARIABLE(runtimeVersion);
                RCF_UNUSED_VARIABLE(archiveVersion);
                RCF_UNUSED_VARIABLE(spIn);

                mAr.reset( new IS(is) );
                invokeCustomizationCallback();
            }

            void unbind()
            {
                mAr.reset();
            }

            template<typename T> In &operator>>(T &t)
            {
                *mAr >> t;
                return *this;
            }

            typedef std::function<void(IS &)> CustomizationCallback;

            void setCustomizationCallback(CustomizationCallback customizationCallback)
            {
                mCustomizationCallback = customizationCallback;
                invokeCustomizationCallback();
            }           

        private:
            std::unique_ptr<IS> mAr;

            CustomizationCallback mCustomizationCallback;

            void invokeCustomizationCallback()
            {
                if (mCustomizationCallback)
                {
                    mCustomizationCallback(*mAr);
                }
            }

        };

        class Out
        {
        public:
            void bind(
                RCF::MemOstream &os, 
                int runtimeVersion, 
                int archiveVersion,
                SerializationProtocolOut & spOut)                           
            {
                RCF_UNUSED_VARIABLE(runtimeVersion);
                RCF_UNUSED_VARIABLE(archiveVersion);
                RCF_UNUSED_VARIABLE(spOut);

                mAr.reset( new OS(os) );
                invokeCustomizationCallback();
            }

            void unbind()
            {
                mAr.reset();
            }

            template<typename T> Out &operator<<(const T &t)
            {
                *mAr << t;
                return *this;
            }

            typedef std::function<void(OS &)> CustomizationCallback;

            void setCustomizationCallback(CustomizationCallback customizationCallback)
            {
                mCustomizationCallback = customizationCallback;
                invokeCustomizationCallback();
            }

        private:
            std::unique_ptr<OS> mAr;

            CustomizationCallback mCustomizationCallback;

            void invokeCustomizationCallback()
            {
                if (mCustomizationCallback)
                {
                    mCustomizationCallback(*mAr);
                }
            }

        };
    };

} // namespace RCF

#endif //! INCLUDE_RCF_PROTOCOL_PROTOCOL_HPP
