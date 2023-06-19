
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

#ifndef INCLUDE_RCF_ANY_HPP
#define INCLUDE_RCF_ANY_HPP

#include <memory>

#include <RCF/Exception.hpp>
#include <RCF/Tools.hpp>

namespace RCF
{
    class AnyHolderBase;
    typedef std::unique_ptr<AnyHolderBase> AnyHolderPtr;

    class AnyHolderBase
    {
    public:
        virtual ~AnyHolderBase() {}
        virtual AnyHolderPtr clone() const = 0;
    };

    template<typename T>
    class AnyHolder : public AnyHolderBase
    {
    public:
        AnyHolder(const T& any) : mAny(any)
        {}

        ~AnyHolder()
        {}

        virtual AnyHolderPtr clone() const
        {
            return AnyHolderPtr(new AnyHolder<T>(mAny));
        }

        T& get()
        {
            return mAny;
        }

    private:
        T mAny;
    };

    /// Generic container type used to hold arbitrary objects.
    class Any
    {
    public:

        /// Constructs an empty Any instance.
        Any()
        {
        }

        /// Constructs an Any instance holding a copy of t.
        template<typename T>
        Any(const T& t) : mAnyPtr(new AnyHolder<T>(t))
        {
        }

        /// Copy constructs an Any instance.
        Any(const Any& rhs)
        {
            if ( rhs.mAnyPtr )
            {
                mAnyPtr = rhs.mAnyPtr->clone();
            }
        }

        /// Assigns an Any instance.
        Any& operator=(const Any& rhs)
        {
            if ( this != &rhs )
            {
                mAnyPtr.reset();
                if ( rhs.mAnyPtr )
                {
                    mAnyPtr = rhs.mAnyPtr->clone();
                }
            }
            return *this;
        }

        /// Type-safe retrieval of the contained value. Throws an exception if T does not match the type of the contained type.
        template<typename T>
        T& get()
        {
            if ( !mAnyPtr )
            {
                // TODO: literal
                RCF_THROW(Exception("Any cast failed. Null value."));
            }
            AnyHolder<T> * pHolder = dynamic_cast<AnyHolder<T> *>(mAnyPtr.get());
            if ( !pHolder )
            {
                // TODO: literal
                RCF_THROW(Exception("Any cast failed. Type mismatch."));
            }
            return pHolder->get();
        }

    private:
        AnyHolderPtr mAnyPtr;
    };

} // namespace RCF

#endif // ! INCLUDE_RCF_ANY_HPP
