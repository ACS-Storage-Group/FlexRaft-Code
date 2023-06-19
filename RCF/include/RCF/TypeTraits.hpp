
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

#ifndef INCLUDE_RCF_TYPETRAITS_HPP
#define INCLUDE_RCF_TYPETRAITS_HPP

#include <type_traits>

namespace RCF {

    typedef std::true_type                                      TrueType;
    typedef std::false_type                                     FalseType;

    template<int N> using Int                                   = std::integral_constant<int, N>;
    template<bool B> using Bool                                 = std::integral_constant<bool, B>;
    template<typename C, typename T1, typename T2> using If     = std::conditional<C::value, T1, T2>;

    //template<typename T1, typename T2> using And              = std::conjunction<T1, T2>;
    
    // Homebrew replacement as std::conjunction not widely available (requires VS 2015 Update 2).
    template<typename T1, typename T2> class AndImpl;
    template<> class AndImpl<TrueType, TrueType> : public TrueType {};
    template<> class AndImpl<TrueType, FalseType> : public FalseType {};
    template<> class AndImpl<FalseType, TrueType> : public FalseType {};
    template<> class AndImpl<FalseType, FalseType> : public FalseType {};
    template<typename T1, typename T2> class And : public AndImpl<typename T1::type, typename T2::type> {};

    //template<typename T1, typename T2> using Or               = std::disjunction<T1, T2>;

    // Homebrew replacement as std::disjunction not widely available (requires VS 2015 Update 2).
    template<typename T1, typename T2> class OrImpl;
    template<> class OrImpl<TrueType, TrueType> : public TrueType {};
    template<> class OrImpl<TrueType, FalseType> : public FalseType {};
    template<> class OrImpl<FalseType, TrueType> : public FalseType {};
    template<> class OrImpl<FalseType, FalseType> : public FalseType {};
    template<typename T1, typename T2> class Or : public OrImpl<typename T1::type, typename T2::type> {};

    //template<typename T1> using Not                           = std::negation<T1, T2>;

    // Homebrew replacement as std::negation not widely available (requires VS 2015 Update 2).
    template<typename T1> class NotImpl;
    template<> class NotImpl<TrueType> : public FalseType {};
    template<> class NotImpl<FalseType> : public TrueType {};
    template<typename T1> class Not : public NotImpl<typename T1::type> {};

    template<typename T> using IsFundamental                    = std::is_fundamental<T>;
    template<typename T> using IsConst                          = std::is_const<T>;
    template<typename T> using IsPointer                        = std::is_pointer<T>;
    template<typename T> using IsReference                      = std::is_reference<T>;
    template<typename T> using RemovePointer                    = std::remove_pointer<T>;
    template<typename T> using RemoveReference                  = std::remove_reference<T>;
    template<typename T> using RemoveCv                         = std::remove_cv<T>;

    template<typename T> struct Out {};

    template<typename T> struct RemoveOut { typedef T type; };
    template<typename T> struct RemoveOut< Out<T> > { typedef T type; };
    
    template<typename T> struct IsOut : public RCF::FalseType {};
    template<typename T> struct IsOut< Out<T> > {
        typedef TrueType type;
        enum { value = type::value };
    };

} // namespace RCF

#endif // ! INCLUDE_RCF_TYPETRAITS_HPP
