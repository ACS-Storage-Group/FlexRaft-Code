
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

#ifndef INCLUDE_RCF_FILTERSERVICE_HPP
#define INCLUDE_RCF_FILTERSERVICE_HPP

#include <map>

#include <memory>

#include <RCF/Filter.hpp>
#include <RCF/Export.hpp>
#include <RCF/Service.hpp>
#include <RCF/Tools.hpp>

namespace RCF {

    class Session;
    class RcfSession;

    class RCF_EXPORT FilterService :
        public I_Service,
        Noncopyable
    {
    public:
        FilterService();
       
        void addFilterFactory(FilterFactoryPtr filterFactoryPtr);

        void addFilterFactory(
            FilterFactoryPtr filterFactoryPtr,
            const std::vector<int> &filterIds);

        std::int32_t RequestTransportFilters(const std::vector<std::int32_t> &filterIds);

        // No longer supported. Just a stub to return an error message.
        std::int32_t QueryForTransportFilters(const std::vector<std::int32_t> &filterIds);

        FilterFactoryPtr getFilterFactoryPtr(int filterId);
   
    private:
        void setTransportFilters(
            RcfSession &session,
            std::shared_ptr<std::vector<FilterPtr> > filters);

        void onServerStart(RcfServer &server);
        void onServerStop(RcfServer &server);

        typedef std::map<int, FilterFactoryPtr>     FilterFactoryMap;
        FilterFactoryMap                            mFilterFactoryMap;
        ReadWriteMutex                              mFilterFactoryMapMutex;
    };

    typedef std::shared_ptr<FilterService> FilterServicePtr;

} // namespace RCF

#endif // ! INCLUDE_RCF_FILTERSERVICE_HPP
