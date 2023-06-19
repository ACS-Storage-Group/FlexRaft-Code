
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

#ifndef INCLUDE_RCF_SERVEROBJECTSERVICE_HPP
#define INCLUDE_RCF_SERVEROBJECTSERVICE_HPP

#include <RCF/Any.hpp>
#include <RCF/Export.hpp>
#include <RCF/PeriodicTimer.hpp>
#include <RCF/Service.hpp>
#include <RCF/Tools.hpp>


namespace RCF {

    class ServerObjectService;
    typedef std::shared_ptr<ServerObjectService> ServerObjectServicePtr;

    class RCF_EXPORT ServerObjectHolder
    {
    public:

        ServerObjectHolder();

        ServerObjectHolder(const Any & serverObject, std::uint32_t timeoutMs);

        std::uint32_t     mTimeoutMs;
        std::uint32_t     mLastTouchMs;
        int                 mUseCount;
        Any          mServerObject;
    };
    
    class RCF_EXPORT ServerObjectService : public I_Service, Noncopyable
    {
    public:
        ServerObjectService();

    private:
        void            onServerStart(RcfServer & server);
        void            onServerStop(RcfServer & server);
        void            onTimer();
        void            customDeleter(const std::string & objectKey, void * pt);

        typedef std::map<std::string, ServerObjectHolder>   ServerObjectMap;

        RcfServer *         mpRcfServer;
        PeriodicTimer       mPeriodicTimer;

        std::uint32_t     mHarvestingIntervalS;
        std::uint32_t     mLastHarvestMs;

        Mutex               mMutex;
        ServerObjectMap     mServerObjectMap;

        

        template<typename T>
        std::shared_ptr<T> getServerObjectImpl(
            const std::string & objectKey, 
            std::uint32_t timeoutMs, 
            bool createIfDoesntExist)
        {
            typedef std::shared_ptr<T> TPtr;

            Lock lock(mMutex);

            ServerObjectMap::iterator iter = mServerObjectMap.find(objectKey);
            if (iter != mServerObjectMap.end())
            {
                ServerObjectHolder & holder = iter->second;
                Any & a = holder.mServerObject;
                TPtr & tPtr = a.get<TPtr>();
                T * pt = tPtr.get();
                RCF_ASSERT(pt);

                // Return shared_ptr with custom deleter.
                holder.mLastTouchMs = getCurrentTimeMs();
                RCF_ASSERT(holder.mUseCount >= 0);
                ++holder.mUseCount;
                TPtr ptr(pt, std::bind(&ServerObjectService::customDeleter, this, objectKey, std::placeholders::_1));
                return ptr;
            }
            else if (createIfDoesntExist)
            {
                T * pt = new T();
                TPtr tPtr(pt);
                mServerObjectMap[objectKey] = ServerObjectHolder(Any(tPtr), timeoutMs);
                ServerObjectHolder & holder = mServerObjectMap[objectKey];

                // Return shared_ptr with custom deleter.
                holder.mLastTouchMs = getCurrentTimeMs();
                RCF_ASSERT(holder.mUseCount >= 0);
                ++holder.mUseCount;
                TPtr ptr(pt, std::bind(&ServerObjectService::customDeleter, this, objectKey, std::placeholders::_1));
                return ptr;
            }
            else
            {
                return TPtr();
            }
        }

    public:

        template<typename T>
        std::shared_ptr<T> queryServerObject(
            const std::string & objectKey)
        {
            return getServerObjectImpl<T>(objectKey, 0, false);
        }

        template<typename T>
        std::shared_ptr<T> getServerObject(
            const std::string & objectKey, 
            std::uint32_t timeoutMs)
        {
            return getServerObjectImpl<T>(objectKey, timeoutMs, true);
        }

        void deleteServerObject(const std::string & objectKey);
    };

} // namespace RCF

#endif // ! INCLUDE_RCF_SERVEROBJECTSERVICE_HPP
