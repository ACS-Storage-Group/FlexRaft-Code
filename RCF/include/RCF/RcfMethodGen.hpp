
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

#ifndef INCLUDE_RCF_RCFMETHODGEN_HPP
#define INCLUDE_RCF_RCFMETHODGEN_HPP




//------------------------------------------------------------------------------
// Parameters - R0
//------------------------------------------------------------------------------

// RCF_METHOD_R0
#define RCF_METHOD_R0(R,func  )                                               \
    RCF_METHOD_R0_(R,func  , RCF_MAKE_UNIQUE_ID(func, R0))

#define RCF_METHOD_R0_(R,func  , id)                                          \
        public:                                                               \
            RCF_MAKE_NEXT_DISPATCH_ID(id)                                     \
            ::RCF::FutureConverter<R > func(                                  \
                )                                                             \
            {                                                                 \
                return func(                                                  \
                    ::RCF::CallOptions()                                      \
                    );                                                        \
            }                                                                 \
            ::RCF::FutureConverter<R > func(                                  \
                const ::RCF::CallOptions &callOptions                         \
                )                                                             \
            {                                                                 \
                checkClientInitialized();                                     \
                getClientStub().setAsync(false);                              \
                return RCF::FutureConverter<R >(                              \
                    ::RCF::AllocateClientParameters<                          \
                        R                                                     \
                         ,                                                    \
                        V,V,V,V,V,V,V,V,V,V,V,V,V,V,V >()(                    \
                            getClientStub()                                   \
                             ,                                                \
                            V(),V(),V(),V(),V(),V(),V(),V(),V(),V(),V(),V(),V(),V(),V()).r.get(),\
                    getClientStub(),                                          \
                    id::value,                                                \
                    callOptions.apply(getClientStub()),                       \
                    #func,                                                    \
                    "R0");                                                    \
            }                                                                 \
            const char * getFunctionName(const id &)                          \
            {                                                                 \
                return #func;                                                 \
            }                                                                 \
            const char * getArity(const id &)                                 \
            {                                                                 \
                return "R0";                                                  \
            }                                                                 \
                                                                              \
        private:                                                              \
            template<typename T>                                              \
            void callMethod(                                                  \
                const id &,                                                   \
                ::RCF::RcfSession &session,                                   \
                T &t)                                                         \
            {                                                                 \
                ::RCF::ServerParameters<                                      \
                    R                                                         \
                     > &p =                                                   \
                    ::RCF::AllocateServerParameters<                          \
                        R                                                     \
                         >()(session);                                        \
                p.r.set(                                                      \
                    session.getAutoSend(),                                    \
                    t.func(                                                   \
                        ));                                                   \
            }




//------------------------------------------------------------------------------
// Parameters - V0
//------------------------------------------------------------------------------

// RCF_METHOD_V0
#define RCF_METHOD_V0(R,func  )                                               \
    RCF_METHOD_V0_(R,func   , RCF_MAKE_UNIQUE_ID(func, V0))

#define RCF_METHOD_V0_(R,func  , id)                                          \
        public:                                                               \
            static_assert( std::is_same<R, void>::value, "RCF_METHOD_Vn() must be used for methods returning void." );\
            RCF_MAKE_NEXT_DISPATCH_ID(id)                                     \
            ::RCF::FutureConverter<V> func(                                   \
                )                                                             \
            {                                                                 \
                return func(                                                  \
                    ::RCF::CallOptions()                                      \
                    );                                                        \
            }                                                                 \
            ::RCF::FutureConverter<V> func(                                   \
                const ::RCF::CallOptions &callOptions                         \
                )                                                             \
            {                                                                 \
                checkClientInitialized();                                     \
                getClientStub().setAsync(false);                              \
                return RCF::FutureConverter<V>(                               \
                    ::RCF::AllocateClientParameters<                          \
                        V                                                     \
                         ,                                                    \
                        V,V,V,V,V,V,V,V,V,V,V,V,V,V,V >()(                    \
                            getClientStub()                                   \
                             ,                                                \
                            V(),V(),V(),V(),V(),V(),V(),V(),V(),V(),V(),V(),V(),V(),V()).r.get(),\
                    getClientStub(),                                          \
                    id::value,                                                \
                    callOptions.apply(getClientStub()),                       \
                    #func,                                                    \
                    "V0");                                                    \
            }                                                                 \
            const char * getFunctionName(const id &)                          \
            {                                                                 \
                return #func;                                                 \
            }                                                                 \
            const char * getArity(const id &)                                 \
            {                                                                 \
                return "V0";                                                  \
            }                                                                 \
                                                                              \
        private:                                                              \
            template<typename T>                                              \
            void callMethod(                                                  \
                const id &,                                                   \
                ::RCF::RcfSession &session,                                   \
                T &t)                                                         \
            {                                                                 \
                ::RCF::ServerParameters<                                      \
                    V                                                         \
                     > &p =                                                   \
                        ::RCF::AllocateServerParameters<                      \
                            V                                                 \
                             >()(session);                                    \
                RCF_UNUSED_VARIABLE(p);                                       \
                t.func(                                                       \
                    );                                                        \
            }




//------------------------------------------------------------------------------
// Parameters - R1
//------------------------------------------------------------------------------

// RCF_METHOD_R1
#define RCF_METHOD_R1(R,func , A1)                                            \
    RCF_METHOD_R1_(R,func , A1, RCF_MAKE_UNIQUE_ID(func, R1))

#define RCF_METHOD_R1_(R,func , A1, id)                                       \
        public:                                                               \
            RCF_MAKE_NEXT_DISPATCH_ID(id)                                     \
            ::RCF::FutureConverter<R > func(                                  \
                ::RCF::RemoveOut<A1 >::type a1)                               \
            {                                                                 \
                return func(                                                  \
                    ::RCF::CallOptions() ,                                    \
                    a1);                                                      \
            }                                                                 \
            ::RCF::FutureConverter<R > func(                                  \
                const ::RCF::CallOptions &callOptions ,                       \
                ::RCF::RemoveOut<A1 >::type a1)                               \
            {                                                                 \
                checkClientInitialized();                                     \
                getClientStub().setAsync(false);                              \
                return RCF::FutureConverter<R >(                              \
                    ::RCF::AllocateClientParameters<                          \
                        R ,                                                   \
                        A1 ,                                                  \
                        V,V,V,V,V,V,V,V,V,V,V,V,V,V >()(                      \
                            getClientStub() ,                                 \
                            a1 ,                                              \
                            V(),V(),V(),V(),V(),V(),V(),V(),V(),V(),V(),V(),V(),V()).r.get(),\
                    getClientStub(),                                          \
                    id::value,                                                \
                    callOptions.apply(getClientStub()),                       \
                    #func,                                                    \
                    "R1");                                                    \
            }                                                                 \
            const char * getFunctionName(const id &)                          \
            {                                                                 \
                return #func;                                                 \
            }                                                                 \
            const char * getArity(const id &)                                 \
            {                                                                 \
                return "R1";                                                  \
            }                                                                 \
                                                                              \
        private:                                                              \
            template<typename T>                                              \
            void callMethod(                                                  \
                const id &,                                                   \
                ::RCF::RcfSession &session,                                   \
                T &t)                                                         \
            {                                                                 \
                ::RCF::ServerParameters<                                      \
                    R ,                                                       \
                    A1 > &p =                                                 \
                    ::RCF::AllocateServerParameters<                          \
                        R ,                                                   \
                        A1 >()(session);                                      \
                p.r.set(                                                      \
                    session.getAutoSend(),                                    \
                    t.func(                                                   \
                        p.a1.get()));                                         \
            }




//------------------------------------------------------------------------------
// Parameters - V1
//------------------------------------------------------------------------------

// RCF_METHOD_V1
#define RCF_METHOD_V1(R,func , A1)                                            \
    RCF_METHOD_V1_(R,func  , A1, RCF_MAKE_UNIQUE_ID(func, V1))

#define RCF_METHOD_V1_(R,func , A1, id)                                       \
        public:                                                               \
            static_assert( std::is_same<R, void>::value, "RCF_METHOD_Vn() must be used for methods returning void." );\
            RCF_MAKE_NEXT_DISPATCH_ID(id)                                     \
            ::RCF::FutureConverter<V> func(                                   \
                ::RCF::RemoveOut<A1 >::type a1)                               \
            {                                                                 \
                return func(                                                  \
                    ::RCF::CallOptions() ,                                    \
                    a1);                                                      \
            }                                                                 \
            ::RCF::FutureConverter<V> func(                                   \
                const ::RCF::CallOptions &callOptions ,                       \
                ::RCF::RemoveOut<A1 >::type a1)                               \
            {                                                                 \
                checkClientInitialized();                                     \
                getClientStub().setAsync(false);                              \
                return RCF::FutureConverter<V>(                               \
                    ::RCF::AllocateClientParameters<                          \
                        V ,                                                   \
                        A1 ,                                                  \
                        V,V,V,V,V,V,V,V,V,V,V,V,V,V >()(                      \
                            getClientStub() ,                                 \
                            a1 ,                                              \
                            V(),V(),V(),V(),V(),V(),V(),V(),V(),V(),V(),V(),V(),V()).r.get(),\
                    getClientStub(),                                          \
                    id::value,                                                \
                    callOptions.apply(getClientStub()),                       \
                    #func,                                                    \
                    "V1");                                                    \
            }                                                                 \
            const char * getFunctionName(const id &)                          \
            {                                                                 \
                return #func;                                                 \
            }                                                                 \
            const char * getArity(const id &)                                 \
            {                                                                 \
                return "V1";                                                  \
            }                                                                 \
                                                                              \
        private:                                                              \
            template<typename T>                                              \
            void callMethod(                                                  \
                const id &,                                                   \
                ::RCF::RcfSession &session,                                   \
                T &t)                                                         \
            {                                                                 \
                ::RCF::ServerParameters<                                      \
                    V ,                                                       \
                    A1 > &p =                                                 \
                        ::RCF::AllocateServerParameters<                      \
                            V ,                                               \
                            A1 >()(session);                                  \
                RCF_UNUSED_VARIABLE(p);                                       \
                t.func(                                                       \
                    p.a1.get());                                              \
            }




//------------------------------------------------------------------------------
// Parameters - R2
//------------------------------------------------------------------------------

// RCF_METHOD_R2
#define RCF_METHOD_R2(R,func , A1,A2)                                         \
    RCF_METHOD_R2_(R,func , A1,A2, RCF_MAKE_UNIQUE_ID(func, R2))

#define RCF_METHOD_R2_(R,func , A1,A2, id)                                    \
        public:                                                               \
            RCF_MAKE_NEXT_DISPATCH_ID(id)                                     \
            ::RCF::FutureConverter<R > func(                                  \
                ::RCF::RemoveOut<A1 >::type a1,                               \
                ::RCF::RemoveOut<A2 >::type a2)                               \
            {                                                                 \
                return func(                                                  \
                    ::RCF::CallOptions() ,                                    \
                    a1,a2);                                                   \
            }                                                                 \
            ::RCF::FutureConverter<R > func(                                  \
                const ::RCF::CallOptions &callOptions ,                       \
                ::RCF::RemoveOut<A1 >::type a1,                               \
                ::RCF::RemoveOut<A2 >::type a2)                               \
            {                                                                 \
                checkClientInitialized();                                     \
                getClientStub().setAsync(false);                              \
                return RCF::FutureConverter<R >(                              \
                    ::RCF::AllocateClientParameters<                          \
                        R ,                                                   \
                        A1,A2 ,                                               \
                        V,V,V,V,V,V,V,V,V,V,V,V,V >()(                        \
                            getClientStub() ,                                 \
                            a1,a2 ,                                           \
                            V(),V(),V(),V(),V(),V(),V(),V(),V(),V(),V(),V(),V()).r.get(),\
                    getClientStub(),                                          \
                    id::value,                                                \
                    callOptions.apply(getClientStub()),                       \
                    #func,                                                    \
                    "R2");                                                    \
            }                                                                 \
            const char * getFunctionName(const id &)                          \
            {                                                                 \
                return #func;                                                 \
            }                                                                 \
            const char * getArity(const id &)                                 \
            {                                                                 \
                return "R2";                                                  \
            }                                                                 \
                                                                              \
        private:                                                              \
            template<typename T>                                              \
            void callMethod(                                                  \
                const id &,                                                   \
                ::RCF::RcfSession &session,                                   \
                T &t)                                                         \
            {                                                                 \
                ::RCF::ServerParameters<                                      \
                    R ,                                                       \
                    A1,A2 > &p =                                              \
                    ::RCF::AllocateServerParameters<                          \
                        R ,                                                   \
                        A1,A2 >()(session);                                   \
                p.r.set(                                                      \
                    session.getAutoSend(),                                    \
                    t.func(                                                   \
                        p.a1.get(),                                           \
                        p.a2.get()));                                         \
            }




//------------------------------------------------------------------------------
// Parameters - V2
//------------------------------------------------------------------------------

// RCF_METHOD_V2
#define RCF_METHOD_V2(R,func , A1,A2)                                         \
    RCF_METHOD_V2_(R,func  , A1,A2, RCF_MAKE_UNIQUE_ID(func, V2))

#define RCF_METHOD_V2_(R,func , A1,A2, id)                                    \
        public:                                                               \
            static_assert( std::is_same<R, void>::value, "RCF_METHOD_Vn() must be used for methods returning void." );\
            RCF_MAKE_NEXT_DISPATCH_ID(id)                                     \
            ::RCF::FutureConverter<V> func(                                   \
                ::RCF::RemoveOut<A1 >::type a1,                               \
                ::RCF::RemoveOut<A2 >::type a2)                               \
            {                                                                 \
                return func(                                                  \
                    ::RCF::CallOptions() ,                                    \
                    a1,a2);                                                   \
            }                                                                 \
            ::RCF::FutureConverter<V> func(                                   \
                const ::RCF::CallOptions &callOptions ,                       \
                ::RCF::RemoveOut<A1 >::type a1,                               \
                ::RCF::RemoveOut<A2 >::type a2)                               \
            {                                                                 \
                checkClientInitialized();                                     \
                getClientStub().setAsync(false);                              \
                return RCF::FutureConverter<V>(                               \
                    ::RCF::AllocateClientParameters<                          \
                        V ,                                                   \
                        A1,A2 ,                                               \
                        V,V,V,V,V,V,V,V,V,V,V,V,V >()(                        \
                            getClientStub() ,                                 \
                            a1,a2 ,                                           \
                            V(),V(),V(),V(),V(),V(),V(),V(),V(),V(),V(),V(),V()).r.get(),\
                    getClientStub(),                                          \
                    id::value,                                                \
                    callOptions.apply(getClientStub()),                       \
                    #func,                                                    \
                    "V2");                                                    \
            }                                                                 \
            const char * getFunctionName(const id &)                          \
            {                                                                 \
                return #func;                                                 \
            }                                                                 \
            const char * getArity(const id &)                                 \
            {                                                                 \
                return "V2";                                                  \
            }                                                                 \
                                                                              \
        private:                                                              \
            template<typename T>                                              \
            void callMethod(                                                  \
                const id &,                                                   \
                ::RCF::RcfSession &session,                                   \
                T &t)                                                         \
            {                                                                 \
                ::RCF::ServerParameters<                                      \
                    V ,                                                       \
                    A1,A2 > &p =                                              \
                        ::RCF::AllocateServerParameters<                      \
                            V ,                                               \
                            A1,A2 >()(session);                               \
                RCF_UNUSED_VARIABLE(p);                                       \
                t.func(                                                       \
                    p.a1.get(),                                               \
                        p.a2.get());                                          \
            }




//------------------------------------------------------------------------------
// Parameters - R3
//------------------------------------------------------------------------------

// RCF_METHOD_R3
#define RCF_METHOD_R3(R,func , A1,A2,A3)                                      \
    RCF_METHOD_R3_(R,func , A1,A2,A3, RCF_MAKE_UNIQUE_ID(func, R3))

#define RCF_METHOD_R3_(R,func , A1,A2,A3, id)                                 \
        public:                                                               \
            RCF_MAKE_NEXT_DISPATCH_ID(id)                                     \
            ::RCF::FutureConverter<R > func(                                  \
                ::RCF::RemoveOut<A1 >::type a1,                               \
                ::RCF::RemoveOut<A2 >::type a2,                               \
                ::RCF::RemoveOut<A3 >::type a3)                               \
            {                                                                 \
                return func(                                                  \
                    ::RCF::CallOptions() ,                                    \
                    a1,a2,a3);                                                \
            }                                                                 \
            ::RCF::FutureConverter<R > func(                                  \
                const ::RCF::CallOptions &callOptions ,                       \
                ::RCF::RemoveOut<A1 >::type a1,                               \
                ::RCF::RemoveOut<A2 >::type a2,                               \
                ::RCF::RemoveOut<A3 >::type a3)                               \
            {                                                                 \
                checkClientInitialized();                                     \
                getClientStub().setAsync(false);                              \
                return RCF::FutureConverter<R >(                              \
                    ::RCF::AllocateClientParameters<                          \
                        R ,                                                   \
                        A1,A2,A3 ,                                            \
                        V,V,V,V,V,V,V,V,V,V,V,V >()(                          \
                            getClientStub() ,                                 \
                            a1,a2,a3 ,                                        \
                            V(),V(),V(),V(),V(),V(),V(),V(),V(),V(),V(),V()).r.get(),\
                    getClientStub(),                                          \
                    id::value,                                                \
                    callOptions.apply(getClientStub()),                       \
                    #func,                                                    \
                    "R3");                                                    \
            }                                                                 \
            const char * getFunctionName(const id &)                          \
            {                                                                 \
                return #func;                                                 \
            }                                                                 \
            const char * getArity(const id &)                                 \
            {                                                                 \
                return "R3";                                                  \
            }                                                                 \
                                                                              \
        private:                                                              \
            template<typename T>                                              \
            void callMethod(                                                  \
                const id &,                                                   \
                ::RCF::RcfSession &session,                                   \
                T &t)                                                         \
            {                                                                 \
                ::RCF::ServerParameters<                                      \
                    R ,                                                       \
                    A1,A2,A3 > &p =                                           \
                    ::RCF::AllocateServerParameters<                          \
                        R ,                                                   \
                        A1,A2,A3 >()(session);                                \
                p.r.set(                                                      \
                    session.getAutoSend(),                                    \
                    t.func(                                                   \
                        p.a1.get(),                                           \
                        p.a2.get(),                                           \
                        p.a3.get()));                                         \
            }




//------------------------------------------------------------------------------
// Parameters - V3
//------------------------------------------------------------------------------

// RCF_METHOD_V3
#define RCF_METHOD_V3(R,func , A1,A2,A3)                                      \
    RCF_METHOD_V3_(R,func  , A1,A2,A3, RCF_MAKE_UNIQUE_ID(func, V3))

#define RCF_METHOD_V3_(R,func , A1,A2,A3, id)                                 \
        public:                                                               \
            static_assert( std::is_same<R, void>::value, "RCF_METHOD_Vn() must be used for methods returning void." );\
            RCF_MAKE_NEXT_DISPATCH_ID(id)                                     \
            ::RCF::FutureConverter<V> func(                                   \
                ::RCF::RemoveOut<A1 >::type a1,                               \
                ::RCF::RemoveOut<A2 >::type a2,                               \
                ::RCF::RemoveOut<A3 >::type a3)                               \
            {                                                                 \
                return func(                                                  \
                    ::RCF::CallOptions() ,                                    \
                    a1,a2,a3);                                                \
            }                                                                 \
            ::RCF::FutureConverter<V> func(                                   \
                const ::RCF::CallOptions &callOptions ,                       \
                ::RCF::RemoveOut<A1 >::type a1,                               \
                ::RCF::RemoveOut<A2 >::type a2,                               \
                ::RCF::RemoveOut<A3 >::type a3)                               \
            {                                                                 \
                checkClientInitialized();                                     \
                getClientStub().setAsync(false);                              \
                return RCF::FutureConverter<V>(                               \
                    ::RCF::AllocateClientParameters<                          \
                        V ,                                                   \
                        A1,A2,A3 ,                                            \
                        V,V,V,V,V,V,V,V,V,V,V,V >()(                          \
                            getClientStub() ,                                 \
                            a1,a2,a3 ,                                        \
                            V(),V(),V(),V(),V(),V(),V(),V(),V(),V(),V(),V()).r.get(),\
                    getClientStub(),                                          \
                    id::value,                                                \
                    callOptions.apply(getClientStub()),                       \
                    #func,                                                    \
                    "V3");                                                    \
            }                                                                 \
            const char * getFunctionName(const id &)                          \
            {                                                                 \
                return #func;                                                 \
            }                                                                 \
            const char * getArity(const id &)                                 \
            {                                                                 \
                return "V3";                                                  \
            }                                                                 \
                                                                              \
        private:                                                              \
            template<typename T>                                              \
            void callMethod(                                                  \
                const id &,                                                   \
                ::RCF::RcfSession &session,                                   \
                T &t)                                                         \
            {                                                                 \
                ::RCF::ServerParameters<                                      \
                    V ,                                                       \
                    A1,A2,A3 > &p =                                           \
                        ::RCF::AllocateServerParameters<                      \
                            V ,                                               \
                            A1,A2,A3 >()(session);                            \
                RCF_UNUSED_VARIABLE(p);                                       \
                t.func(                                                       \
                    p.a1.get(),                                               \
                        p.a2.get(),                                           \
                        p.a3.get());                                          \
            }




//------------------------------------------------------------------------------
// Parameters - R4
//------------------------------------------------------------------------------

// RCF_METHOD_R4
#define RCF_METHOD_R4(R,func , A1,A2,A3,A4)                                   \
    RCF_METHOD_R4_(R,func , A1,A2,A3,A4, RCF_MAKE_UNIQUE_ID(func, R4))

#define RCF_METHOD_R4_(R,func , A1,A2,A3,A4, id)                              \
        public:                                                               \
            RCF_MAKE_NEXT_DISPATCH_ID(id)                                     \
            ::RCF::FutureConverter<R > func(                                  \
                ::RCF::RemoveOut<A1 >::type a1,                               \
                ::RCF::RemoveOut<A2 >::type a2,                               \
                ::RCF::RemoveOut<A3 >::type a3,                               \
                ::RCF::RemoveOut<A4 >::type a4)                               \
            {                                                                 \
                return func(                                                  \
                    ::RCF::CallOptions() ,                                    \
                    a1,a2,a3,a4);                                             \
            }                                                                 \
            ::RCF::FutureConverter<R > func(                                  \
                const ::RCF::CallOptions &callOptions ,                       \
                ::RCF::RemoveOut<A1 >::type a1,                               \
                ::RCF::RemoveOut<A2 >::type a2,                               \
                ::RCF::RemoveOut<A3 >::type a3,                               \
                ::RCF::RemoveOut<A4 >::type a4)                               \
            {                                                                 \
                checkClientInitialized();                                     \
                getClientStub().setAsync(false);                              \
                return RCF::FutureConverter<R >(                              \
                    ::RCF::AllocateClientParameters<                          \
                        R ,                                                   \
                        A1,A2,A3,A4 ,                                         \
                        V,V,V,V,V,V,V,V,V,V,V >()(                            \
                            getClientStub() ,                                 \
                            a1,a2,a3,a4 ,                                     \
                            V(),V(),V(),V(),V(),V(),V(),V(),V(),V(),V()).r.get(),\
                    getClientStub(),                                          \
                    id::value,                                                \
                    callOptions.apply(getClientStub()),                       \
                    #func,                                                    \
                    "R4");                                                    \
            }                                                                 \
            const char * getFunctionName(const id &)                          \
            {                                                                 \
                return #func;                                                 \
            }                                                                 \
            const char * getArity(const id &)                                 \
            {                                                                 \
                return "R4";                                                  \
            }                                                                 \
                                                                              \
        private:                                                              \
            template<typename T>                                              \
            void callMethod(                                                  \
                const id &,                                                   \
                ::RCF::RcfSession &session,                                   \
                T &t)                                                         \
            {                                                                 \
                ::RCF::ServerParameters<                                      \
                    R ,                                                       \
                    A1,A2,A3,A4 > &p =                                        \
                    ::RCF::AllocateServerParameters<                          \
                        R ,                                                   \
                        A1,A2,A3,A4 >()(session);                             \
                p.r.set(                                                      \
                    session.getAutoSend(),                                    \
                    t.func(                                                   \
                        p.a1.get(),                                           \
                        p.a2.get(),                                           \
                        p.a3.get(),                                           \
                        p.a4.get()));                                         \
            }




//------------------------------------------------------------------------------
// Parameters - V4
//------------------------------------------------------------------------------

// RCF_METHOD_V4
#define RCF_METHOD_V4(R,func , A1,A2,A3,A4)                                   \
    RCF_METHOD_V4_(R,func  , A1,A2,A3,A4, RCF_MAKE_UNIQUE_ID(func, V4))

#define RCF_METHOD_V4_(R,func , A1,A2,A3,A4, id)                              \
        public:                                                               \
            static_assert( std::is_same<R, void>::value, "RCF_METHOD_Vn() must be used for methods returning void." );\
            RCF_MAKE_NEXT_DISPATCH_ID(id)                                     \
            ::RCF::FutureConverter<V> func(                                   \
                ::RCF::RemoveOut<A1 >::type a1,                               \
                ::RCF::RemoveOut<A2 >::type a2,                               \
                ::RCF::RemoveOut<A3 >::type a3,                               \
                ::RCF::RemoveOut<A4 >::type a4)                               \
            {                                                                 \
                return func(                                                  \
                    ::RCF::CallOptions() ,                                    \
                    a1,a2,a3,a4);                                             \
            }                                                                 \
            ::RCF::FutureConverter<V> func(                                   \
                const ::RCF::CallOptions &callOptions ,                       \
                ::RCF::RemoveOut<A1 >::type a1,                               \
                ::RCF::RemoveOut<A2 >::type a2,                               \
                ::RCF::RemoveOut<A3 >::type a3,                               \
                ::RCF::RemoveOut<A4 >::type a4)                               \
            {                                                                 \
                checkClientInitialized();                                     \
                getClientStub().setAsync(false);                              \
                return RCF::FutureConverter<V>(                               \
                    ::RCF::AllocateClientParameters<                          \
                        V ,                                                   \
                        A1,A2,A3,A4 ,                                         \
                        V,V,V,V,V,V,V,V,V,V,V >()(                            \
                            getClientStub() ,                                 \
                            a1,a2,a3,a4 ,                                     \
                            V(),V(),V(),V(),V(),V(),V(),V(),V(),V(),V()).r.get(),\
                    getClientStub(),                                          \
                    id::value,                                                \
                    callOptions.apply(getClientStub()),                       \
                    #func,                                                    \
                    "V4");                                                    \
            }                                                                 \
            const char * getFunctionName(const id &)                          \
            {                                                                 \
                return #func;                                                 \
            }                                                                 \
            const char * getArity(const id &)                                 \
            {                                                                 \
                return "V4";                                                  \
            }                                                                 \
                                                                              \
        private:                                                              \
            template<typename T>                                              \
            void callMethod(                                                  \
                const id &,                                                   \
                ::RCF::RcfSession &session,                                   \
                T &t)                                                         \
            {                                                                 \
                ::RCF::ServerParameters<                                      \
                    V ,                                                       \
                    A1,A2,A3,A4 > &p =                                        \
                        ::RCF::AllocateServerParameters<                      \
                            V ,                                               \
                            A1,A2,A3,A4 >()(session);                         \
                RCF_UNUSED_VARIABLE(p);                                       \
                t.func(                                                       \
                    p.a1.get(),                                               \
                        p.a2.get(),                                           \
                        p.a3.get(),                                           \
                        p.a4.get());                                          \
            }




//------------------------------------------------------------------------------
// Parameters - R5
//------------------------------------------------------------------------------

// RCF_METHOD_R5
#define RCF_METHOD_R5(R,func , A1,A2,A3,A4,A5)                                \
    RCF_METHOD_R5_(R,func , A1,A2,A3,A4,A5, RCF_MAKE_UNIQUE_ID(func, R5))

#define RCF_METHOD_R5_(R,func , A1,A2,A3,A4,A5, id)                           \
        public:                                                               \
            RCF_MAKE_NEXT_DISPATCH_ID(id)                                     \
            ::RCF::FutureConverter<R > func(                                  \
                ::RCF::RemoveOut<A1 >::type a1,                               \
                ::RCF::RemoveOut<A2 >::type a2,                               \
                ::RCF::RemoveOut<A3 >::type a3,                               \
                ::RCF::RemoveOut<A4 >::type a4,                               \
                ::RCF::RemoveOut<A5 >::type a5)                               \
            {                                                                 \
                return func(                                                  \
                    ::RCF::CallOptions() ,                                    \
                    a1,a2,a3,a4,a5);                                          \
            }                                                                 \
            ::RCF::FutureConverter<R > func(                                  \
                const ::RCF::CallOptions &callOptions ,                       \
                ::RCF::RemoveOut<A1 >::type a1,                               \
                ::RCF::RemoveOut<A2 >::type a2,                               \
                ::RCF::RemoveOut<A3 >::type a3,                               \
                ::RCF::RemoveOut<A4 >::type a4,                               \
                ::RCF::RemoveOut<A5 >::type a5)                               \
            {                                                                 \
                checkClientInitialized();                                     \
                getClientStub().setAsync(false);                              \
                return RCF::FutureConverter<R >(                              \
                    ::RCF::AllocateClientParameters<                          \
                        R ,                                                   \
                        A1,A2,A3,A4,A5 ,                                      \
                        V,V,V,V,V,V,V,V,V,V >()(                              \
                            getClientStub() ,                                 \
                            a1,a2,a3,a4,a5 ,                                  \
                            V(),V(),V(),V(),V(),V(),V(),V(),V(),V()).r.get(), \
                    getClientStub(),                                          \
                    id::value,                                                \
                    callOptions.apply(getClientStub()),                       \
                    #func,                                                    \
                    "R5");                                                    \
            }                                                                 \
            const char * getFunctionName(const id &)                          \
            {                                                                 \
                return #func;                                                 \
            }                                                                 \
            const char * getArity(const id &)                                 \
            {                                                                 \
                return "R5";                                                  \
            }                                                                 \
                                                                              \
        private:                                                              \
            template<typename T>                                              \
            void callMethod(                                                  \
                const id &,                                                   \
                ::RCF::RcfSession &session,                                   \
                T &t)                                                         \
            {                                                                 \
                ::RCF::ServerParameters<                                      \
                    R ,                                                       \
                    A1,A2,A3,A4,A5 > &p =                                     \
                    ::RCF::AllocateServerParameters<                          \
                        R ,                                                   \
                        A1,A2,A3,A4,A5 >()(session);                          \
                p.r.set(                                                      \
                    session.getAutoSend(),                                    \
                    t.func(                                                   \
                        p.a1.get(),                                           \
                        p.a2.get(),                                           \
                        p.a3.get(),                                           \
                        p.a4.get(),                                           \
                        p.a5.get()));                                         \
            }




//------------------------------------------------------------------------------
// Parameters - V5
//------------------------------------------------------------------------------

// RCF_METHOD_V5
#define RCF_METHOD_V5(R,func , A1,A2,A3,A4,A5)                                \
    RCF_METHOD_V5_(R,func  , A1,A2,A3,A4,A5, RCF_MAKE_UNIQUE_ID(func, V5))

#define RCF_METHOD_V5_(R,func , A1,A2,A3,A4,A5, id)                           \
        public:                                                               \
            static_assert( std::is_same<R, void>::value, "RCF_METHOD_Vn() must be used for methods returning void." );\
            RCF_MAKE_NEXT_DISPATCH_ID(id)                                     \
            ::RCF::FutureConverter<V> func(                                   \
                ::RCF::RemoveOut<A1 >::type a1,                               \
                ::RCF::RemoveOut<A2 >::type a2,                               \
                ::RCF::RemoveOut<A3 >::type a3,                               \
                ::RCF::RemoveOut<A4 >::type a4,                               \
                ::RCF::RemoveOut<A5 >::type a5)                               \
            {                                                                 \
                return func(                                                  \
                    ::RCF::CallOptions() ,                                    \
                    a1,a2,a3,a4,a5);                                          \
            }                                                                 \
            ::RCF::FutureConverter<V> func(                                   \
                const ::RCF::CallOptions &callOptions ,                       \
                ::RCF::RemoveOut<A1 >::type a1,                               \
                ::RCF::RemoveOut<A2 >::type a2,                               \
                ::RCF::RemoveOut<A3 >::type a3,                               \
                ::RCF::RemoveOut<A4 >::type a4,                               \
                ::RCF::RemoveOut<A5 >::type a5)                               \
            {                                                                 \
                checkClientInitialized();                                     \
                getClientStub().setAsync(false);                              \
                return RCF::FutureConverter<V>(                               \
                    ::RCF::AllocateClientParameters<                          \
                        V ,                                                   \
                        A1,A2,A3,A4,A5 ,                                      \
                        V,V,V,V,V,V,V,V,V,V >()(                              \
                            getClientStub() ,                                 \
                            a1,a2,a3,a4,a5 ,                                  \
                            V(),V(),V(),V(),V(),V(),V(),V(),V(),V()).r.get(), \
                    getClientStub(),                                          \
                    id::value,                                                \
                    callOptions.apply(getClientStub()),                       \
                    #func,                                                    \
                    "V5");                                                    \
            }                                                                 \
            const char * getFunctionName(const id &)                          \
            {                                                                 \
                return #func;                                                 \
            }                                                                 \
            const char * getArity(const id &)                                 \
            {                                                                 \
                return "V5";                                                  \
            }                                                                 \
                                                                              \
        private:                                                              \
            template<typename T>                                              \
            void callMethod(                                                  \
                const id &,                                                   \
                ::RCF::RcfSession &session,                                   \
                T &t)                                                         \
            {                                                                 \
                ::RCF::ServerParameters<                                      \
                    V ,                                                       \
                    A1,A2,A3,A4,A5 > &p =                                     \
                        ::RCF::AllocateServerParameters<                      \
                            V ,                                               \
                            A1,A2,A3,A4,A5 >()(session);                      \
                RCF_UNUSED_VARIABLE(p);                                       \
                t.func(                                                       \
                    p.a1.get(),                                               \
                        p.a2.get(),                                           \
                        p.a3.get(),                                           \
                        p.a4.get(),                                           \
                        p.a5.get());                                          \
            }




//------------------------------------------------------------------------------
// Parameters - R6
//------------------------------------------------------------------------------

// RCF_METHOD_R6
#define RCF_METHOD_R6(R,func , A1,A2,A3,A4,A5,A6)                             \
    RCF_METHOD_R6_(R,func , A1,A2,A3,A4,A5,A6, RCF_MAKE_UNIQUE_ID(func, R6))

#define RCF_METHOD_R6_(R,func , A1,A2,A3,A4,A5,A6, id)                        \
        public:                                                               \
            RCF_MAKE_NEXT_DISPATCH_ID(id)                                     \
            ::RCF::FutureConverter<R > func(                                  \
                ::RCF::RemoveOut<A1 >::type a1,                               \
                ::RCF::RemoveOut<A2 >::type a2,                               \
                ::RCF::RemoveOut<A3 >::type a3,                               \
                ::RCF::RemoveOut<A4 >::type a4,                               \
                ::RCF::RemoveOut<A5 >::type a5,                               \
                ::RCF::RemoveOut<A6 >::type a6)                               \
            {                                                                 \
                return func(                                                  \
                    ::RCF::CallOptions() ,                                    \
                    a1,a2,a3,a4,a5,a6);                                       \
            }                                                                 \
            ::RCF::FutureConverter<R > func(                                  \
                const ::RCF::CallOptions &callOptions ,                       \
                ::RCF::RemoveOut<A1 >::type a1,                               \
                ::RCF::RemoveOut<A2 >::type a2,                               \
                ::RCF::RemoveOut<A3 >::type a3,                               \
                ::RCF::RemoveOut<A4 >::type a4,                               \
                ::RCF::RemoveOut<A5 >::type a5,                               \
                ::RCF::RemoveOut<A6 >::type a6)                               \
            {                                                                 \
                checkClientInitialized();                                     \
                getClientStub().setAsync(false);                              \
                return RCF::FutureConverter<R >(                              \
                    ::RCF::AllocateClientParameters<                          \
                        R ,                                                   \
                        A1,A2,A3,A4,A5,A6 ,                                   \
                        V,V,V,V,V,V,V,V,V >()(                                \
                            getClientStub() ,                                 \
                            a1,a2,a3,a4,a5,a6 ,                               \
                            V(),V(),V(),V(),V(),V(),V(),V(),V()).r.get(),     \
                    getClientStub(),                                          \
                    id::value,                                                \
                    callOptions.apply(getClientStub()),                       \
                    #func,                                                    \
                    "R6");                                                    \
            }                                                                 \
            const char * getFunctionName(const id &)                          \
            {                                                                 \
                return #func;                                                 \
            }                                                                 \
            const char * getArity(const id &)                                 \
            {                                                                 \
                return "R6";                                                  \
            }                                                                 \
                                                                              \
        private:                                                              \
            template<typename T>                                              \
            void callMethod(                                                  \
                const id &,                                                   \
                ::RCF::RcfSession &session,                                   \
                T &t)                                                         \
            {                                                                 \
                ::RCF::ServerParameters<                                      \
                    R ,                                                       \
                    A1,A2,A3,A4,A5,A6 > &p =                                  \
                    ::RCF::AllocateServerParameters<                          \
                        R ,                                                   \
                        A1,A2,A3,A4,A5,A6 >()(session);                       \
                p.r.set(                                                      \
                    session.getAutoSend(),                                    \
                    t.func(                                                   \
                        p.a1.get(),                                           \
                        p.a2.get(),                                           \
                        p.a3.get(),                                           \
                        p.a4.get(),                                           \
                        p.a5.get(),                                           \
                        p.a6.get()));                                         \
            }




//------------------------------------------------------------------------------
// Parameters - V6
//------------------------------------------------------------------------------

// RCF_METHOD_V6
#define RCF_METHOD_V6(R,func , A1,A2,A3,A4,A5,A6)                             \
    RCF_METHOD_V6_(R,func  , A1,A2,A3,A4,A5,A6, RCF_MAKE_UNIQUE_ID(func, V6))

#define RCF_METHOD_V6_(R,func , A1,A2,A3,A4,A5,A6, id)                        \
        public:                                                               \
            static_assert( std::is_same<R, void>::value, "RCF_METHOD_Vn() must be used for methods returning void." );\
            RCF_MAKE_NEXT_DISPATCH_ID(id)                                     \
            ::RCF::FutureConverter<V> func(                                   \
                ::RCF::RemoveOut<A1 >::type a1,                               \
                ::RCF::RemoveOut<A2 >::type a2,                               \
                ::RCF::RemoveOut<A3 >::type a3,                               \
                ::RCF::RemoveOut<A4 >::type a4,                               \
                ::RCF::RemoveOut<A5 >::type a5,                               \
                ::RCF::RemoveOut<A6 >::type a6)                               \
            {                                                                 \
                return func(                                                  \
                    ::RCF::CallOptions() ,                                    \
                    a1,a2,a3,a4,a5,a6);                                       \
            }                                                                 \
            ::RCF::FutureConverter<V> func(                                   \
                const ::RCF::CallOptions &callOptions ,                       \
                ::RCF::RemoveOut<A1 >::type a1,                               \
                ::RCF::RemoveOut<A2 >::type a2,                               \
                ::RCF::RemoveOut<A3 >::type a3,                               \
                ::RCF::RemoveOut<A4 >::type a4,                               \
                ::RCF::RemoveOut<A5 >::type a5,                               \
                ::RCF::RemoveOut<A6 >::type a6)                               \
            {                                                                 \
                checkClientInitialized();                                     \
                getClientStub().setAsync(false);                              \
                return RCF::FutureConverter<V>(                               \
                    ::RCF::AllocateClientParameters<                          \
                        V ,                                                   \
                        A1,A2,A3,A4,A5,A6 ,                                   \
                        V,V,V,V,V,V,V,V,V >()(                                \
                            getClientStub() ,                                 \
                            a1,a2,a3,a4,a5,a6 ,                               \
                            V(),V(),V(),V(),V(),V(),V(),V(),V()).r.get(),     \
                    getClientStub(),                                          \
                    id::value,                                                \
                    callOptions.apply(getClientStub()),                       \
                    #func,                                                    \
                    "V6");                                                    \
            }                                                                 \
            const char * getFunctionName(const id &)                          \
            {                                                                 \
                return #func;                                                 \
            }                                                                 \
            const char * getArity(const id &)                                 \
            {                                                                 \
                return "V6";                                                  \
            }                                                                 \
                                                                              \
        private:                                                              \
            template<typename T>                                              \
            void callMethod(                                                  \
                const id &,                                                   \
                ::RCF::RcfSession &session,                                   \
                T &t)                                                         \
            {                                                                 \
                ::RCF::ServerParameters<                                      \
                    V ,                                                       \
                    A1,A2,A3,A4,A5,A6 > &p =                                  \
                        ::RCF::AllocateServerParameters<                      \
                            V ,                                               \
                            A1,A2,A3,A4,A5,A6 >()(session);                   \
                RCF_UNUSED_VARIABLE(p);                                       \
                t.func(                                                       \
                    p.a1.get(),                                               \
                        p.a2.get(),                                           \
                        p.a3.get(),                                           \
                        p.a4.get(),                                           \
                        p.a5.get(),                                           \
                        p.a6.get());                                          \
            }




//------------------------------------------------------------------------------
// Parameters - R7
//------------------------------------------------------------------------------

// RCF_METHOD_R7
#define RCF_METHOD_R7(R,func , A1,A2,A3,A4,A5,A6,A7)                          \
    RCF_METHOD_R7_(R,func , A1,A2,A3,A4,A5,A6,A7, RCF_MAKE_UNIQUE_ID(func, R7))

#define RCF_METHOD_R7_(R,func , A1,A2,A3,A4,A5,A6,A7, id)                     \
        public:                                                               \
            RCF_MAKE_NEXT_DISPATCH_ID(id)                                     \
            ::RCF::FutureConverter<R > func(                                  \
                ::RCF::RemoveOut<A1 >::type a1,                               \
                ::RCF::RemoveOut<A2 >::type a2,                               \
                ::RCF::RemoveOut<A3 >::type a3,                               \
                ::RCF::RemoveOut<A4 >::type a4,                               \
                ::RCF::RemoveOut<A5 >::type a5,                               \
                ::RCF::RemoveOut<A6 >::type a6,                               \
                ::RCF::RemoveOut<A7 >::type a7)                               \
            {                                                                 \
                return func(                                                  \
                    ::RCF::CallOptions() ,                                    \
                    a1,a2,a3,a4,a5,a6,a7);                                    \
            }                                                                 \
            ::RCF::FutureConverter<R > func(                                  \
                const ::RCF::CallOptions &callOptions ,                       \
                ::RCF::RemoveOut<A1 >::type a1,                               \
                ::RCF::RemoveOut<A2 >::type a2,                               \
                ::RCF::RemoveOut<A3 >::type a3,                               \
                ::RCF::RemoveOut<A4 >::type a4,                               \
                ::RCF::RemoveOut<A5 >::type a5,                               \
                ::RCF::RemoveOut<A6 >::type a6,                               \
                ::RCF::RemoveOut<A7 >::type a7)                               \
            {                                                                 \
                checkClientInitialized();                                     \
                getClientStub().setAsync(false);                              \
                return RCF::FutureConverter<R >(                              \
                    ::RCF::AllocateClientParameters<                          \
                        R ,                                                   \
                        A1,A2,A3,A4,A5,A6,A7 ,                                \
                        V,V,V,V,V,V,V,V >()(                                  \
                            getClientStub() ,                                 \
                            a1,a2,a3,a4,a5,a6,a7 ,                            \
                            V(),V(),V(),V(),V(),V(),V(),V()).r.get(),         \
                    getClientStub(),                                          \
                    id::value,                                                \
                    callOptions.apply(getClientStub()),                       \
                    #func,                                                    \
                    "R7");                                                    \
            }                                                                 \
            const char * getFunctionName(const id &)                          \
            {                                                                 \
                return #func;                                                 \
            }                                                                 \
            const char * getArity(const id &)                                 \
            {                                                                 \
                return "R7";                                                  \
            }                                                                 \
                                                                              \
        private:                                                              \
            template<typename T>                                              \
            void callMethod(                                                  \
                const id &,                                                   \
                ::RCF::RcfSession &session,                                   \
                T &t)                                                         \
            {                                                                 \
                ::RCF::ServerParameters<                                      \
                    R ,                                                       \
                    A1,A2,A3,A4,A5,A6,A7 > &p =                               \
                    ::RCF::AllocateServerParameters<                          \
                        R ,                                                   \
                        A1,A2,A3,A4,A5,A6,A7 >()(session);                    \
                p.r.set(                                                      \
                    session.getAutoSend(),                                    \
                    t.func(                                                   \
                        p.a1.get(),                                           \
                        p.a2.get(),                                           \
                        p.a3.get(),                                           \
                        p.a4.get(),                                           \
                        p.a5.get(),                                           \
                        p.a6.get(),                                           \
                        p.a7.get()));                                         \
            }




//------------------------------------------------------------------------------
// Parameters - V7
//------------------------------------------------------------------------------

// RCF_METHOD_V7
#define RCF_METHOD_V7(R,func , A1,A2,A3,A4,A5,A6,A7)                          \
    RCF_METHOD_V7_(R,func  , A1,A2,A3,A4,A5,A6,A7, RCF_MAKE_UNIQUE_ID(func, V7))

#define RCF_METHOD_V7_(R,func , A1,A2,A3,A4,A5,A6,A7, id)                     \
        public:                                                               \
            static_assert( std::is_same<R, void>::value, "RCF_METHOD_Vn() must be used for methods returning void." );\
            RCF_MAKE_NEXT_DISPATCH_ID(id)                                     \
            ::RCF::FutureConverter<V> func(                                   \
                ::RCF::RemoveOut<A1 >::type a1,                               \
                ::RCF::RemoveOut<A2 >::type a2,                               \
                ::RCF::RemoveOut<A3 >::type a3,                               \
                ::RCF::RemoveOut<A4 >::type a4,                               \
                ::RCF::RemoveOut<A5 >::type a5,                               \
                ::RCF::RemoveOut<A6 >::type a6,                               \
                ::RCF::RemoveOut<A7 >::type a7)                               \
            {                                                                 \
                return func(                                                  \
                    ::RCF::CallOptions() ,                                    \
                    a1,a2,a3,a4,a5,a6,a7);                                    \
            }                                                                 \
            ::RCF::FutureConverter<V> func(                                   \
                const ::RCF::CallOptions &callOptions ,                       \
                ::RCF::RemoveOut<A1 >::type a1,                               \
                ::RCF::RemoveOut<A2 >::type a2,                               \
                ::RCF::RemoveOut<A3 >::type a3,                               \
                ::RCF::RemoveOut<A4 >::type a4,                               \
                ::RCF::RemoveOut<A5 >::type a5,                               \
                ::RCF::RemoveOut<A6 >::type a6,                               \
                ::RCF::RemoveOut<A7 >::type a7)                               \
            {                                                                 \
                checkClientInitialized();                                     \
                getClientStub().setAsync(false);                              \
                return RCF::FutureConverter<V>(                               \
                    ::RCF::AllocateClientParameters<                          \
                        V ,                                                   \
                        A1,A2,A3,A4,A5,A6,A7 ,                                \
                        V,V,V,V,V,V,V,V >()(                                  \
                            getClientStub() ,                                 \
                            a1,a2,a3,a4,a5,a6,a7 ,                            \
                            V(),V(),V(),V(),V(),V(),V(),V()).r.get(),         \
                    getClientStub(),                                          \
                    id::value,                                                \
                    callOptions.apply(getClientStub()),                       \
                    #func,                                                    \
                    "V7");                                                    \
            }                                                                 \
            const char * getFunctionName(const id &)                          \
            {                                                                 \
                return #func;                                                 \
            }                                                                 \
            const char * getArity(const id &)                                 \
            {                                                                 \
                return "V7";                                                  \
            }                                                                 \
                                                                              \
        private:                                                              \
            template<typename T>                                              \
            void callMethod(                                                  \
                const id &,                                                   \
                ::RCF::RcfSession &session,                                   \
                T &t)                                                         \
            {                                                                 \
                ::RCF::ServerParameters<                                      \
                    V ,                                                       \
                    A1,A2,A3,A4,A5,A6,A7 > &p =                               \
                        ::RCF::AllocateServerParameters<                      \
                            V ,                                               \
                            A1,A2,A3,A4,A5,A6,A7 >()(session);                \
                RCF_UNUSED_VARIABLE(p);                                       \
                t.func(                                                       \
                    p.a1.get(),                                               \
                        p.a2.get(),                                           \
                        p.a3.get(),                                           \
                        p.a4.get(),                                           \
                        p.a5.get(),                                           \
                        p.a6.get(),                                           \
                        p.a7.get());                                          \
            }




//------------------------------------------------------------------------------
// Parameters - R8
//------------------------------------------------------------------------------

// RCF_METHOD_R8
#define RCF_METHOD_R8(R,func , A1,A2,A3,A4,A5,A6,A7,A8)                       \
    RCF_METHOD_R8_(R,func , A1,A2,A3,A4,A5,A6,A7,A8, RCF_MAKE_UNIQUE_ID(func, R8))

#define RCF_METHOD_R8_(R,func , A1,A2,A3,A4,A5,A6,A7,A8, id)                  \
        public:                                                               \
            RCF_MAKE_NEXT_DISPATCH_ID(id)                                     \
            ::RCF::FutureConverter<R > func(                                  \
                ::RCF::RemoveOut<A1 >::type a1,                               \
                ::RCF::RemoveOut<A2 >::type a2,                               \
                ::RCF::RemoveOut<A3 >::type a3,                               \
                ::RCF::RemoveOut<A4 >::type a4,                               \
                ::RCF::RemoveOut<A5 >::type a5,                               \
                ::RCF::RemoveOut<A6 >::type a6,                               \
                ::RCF::RemoveOut<A7 >::type a7,                               \
                ::RCF::RemoveOut<A8 >::type a8)                               \
            {                                                                 \
                return func(                                                  \
                    ::RCF::CallOptions() ,                                    \
                    a1,a2,a3,a4,a5,a6,a7,a8);                                 \
            }                                                                 \
            ::RCF::FutureConverter<R > func(                                  \
                const ::RCF::CallOptions &callOptions ,                       \
                ::RCF::RemoveOut<A1 >::type a1,                               \
                ::RCF::RemoveOut<A2 >::type a2,                               \
                ::RCF::RemoveOut<A3 >::type a3,                               \
                ::RCF::RemoveOut<A4 >::type a4,                               \
                ::RCF::RemoveOut<A5 >::type a5,                               \
                ::RCF::RemoveOut<A6 >::type a6,                               \
                ::RCF::RemoveOut<A7 >::type a7,                               \
                ::RCF::RemoveOut<A8 >::type a8)                               \
            {                                                                 \
                checkClientInitialized();                                     \
                getClientStub().setAsync(false);                              \
                return RCF::FutureConverter<R >(                              \
                    ::RCF::AllocateClientParameters<                          \
                        R ,                                                   \
                        A1,A2,A3,A4,A5,A6,A7,A8 ,                             \
                        V,V,V,V,V,V,V >()(                                    \
                            getClientStub() ,                                 \
                            a1,a2,a3,a4,a5,a6,a7,a8 ,                         \
                            V(),V(),V(),V(),V(),V(),V()).r.get(),             \
                    getClientStub(),                                          \
                    id::value,                                                \
                    callOptions.apply(getClientStub()),                       \
                    #func,                                                    \
                    "R8");                                                    \
            }                                                                 \
            const char * getFunctionName(const id &)                          \
            {                                                                 \
                return #func;                                                 \
            }                                                                 \
            const char * getArity(const id &)                                 \
            {                                                                 \
                return "R8";                                                  \
            }                                                                 \
                                                                              \
        private:                                                              \
            template<typename T>                                              \
            void callMethod(                                                  \
                const id &,                                                   \
                ::RCF::RcfSession &session,                                   \
                T &t)                                                         \
            {                                                                 \
                ::RCF::ServerParameters<                                      \
                    R ,                                                       \
                    A1,A2,A3,A4,A5,A6,A7,A8 > &p =                            \
                    ::RCF::AllocateServerParameters<                          \
                        R ,                                                   \
                        A1,A2,A3,A4,A5,A6,A7,A8 >()(session);                 \
                p.r.set(                                                      \
                    session.getAutoSend(),                                    \
                    t.func(                                                   \
                        p.a1.get(),                                           \
                        p.a2.get(),                                           \
                        p.a3.get(),                                           \
                        p.a4.get(),                                           \
                        p.a5.get(),                                           \
                        p.a6.get(),                                           \
                        p.a7.get(),                                           \
                        p.a8.get()));                                         \
            }




//------------------------------------------------------------------------------
// Parameters - V8
//------------------------------------------------------------------------------

// RCF_METHOD_V8
#define RCF_METHOD_V8(R,func , A1,A2,A3,A4,A5,A6,A7,A8)                       \
    RCF_METHOD_V8_(R,func  , A1,A2,A3,A4,A5,A6,A7,A8, RCF_MAKE_UNIQUE_ID(func, V8))

#define RCF_METHOD_V8_(R,func , A1,A2,A3,A4,A5,A6,A7,A8, id)                  \
        public:                                                               \
            static_assert( std::is_same<R, void>::value, "RCF_METHOD_Vn() must be used for methods returning void." );\
            RCF_MAKE_NEXT_DISPATCH_ID(id)                                     \
            ::RCF::FutureConverter<V> func(                                   \
                ::RCF::RemoveOut<A1 >::type a1,                               \
                ::RCF::RemoveOut<A2 >::type a2,                               \
                ::RCF::RemoveOut<A3 >::type a3,                               \
                ::RCF::RemoveOut<A4 >::type a4,                               \
                ::RCF::RemoveOut<A5 >::type a5,                               \
                ::RCF::RemoveOut<A6 >::type a6,                               \
                ::RCF::RemoveOut<A7 >::type a7,                               \
                ::RCF::RemoveOut<A8 >::type a8)                               \
            {                                                                 \
                return func(                                                  \
                    ::RCF::CallOptions() ,                                    \
                    a1,a2,a3,a4,a5,a6,a7,a8);                                 \
            }                                                                 \
            ::RCF::FutureConverter<V> func(                                   \
                const ::RCF::CallOptions &callOptions ,                       \
                ::RCF::RemoveOut<A1 >::type a1,                               \
                ::RCF::RemoveOut<A2 >::type a2,                               \
                ::RCF::RemoveOut<A3 >::type a3,                               \
                ::RCF::RemoveOut<A4 >::type a4,                               \
                ::RCF::RemoveOut<A5 >::type a5,                               \
                ::RCF::RemoveOut<A6 >::type a6,                               \
                ::RCF::RemoveOut<A7 >::type a7,                               \
                ::RCF::RemoveOut<A8 >::type a8)                               \
            {                                                                 \
                checkClientInitialized();                                     \
                getClientStub().setAsync(false);                              \
                return RCF::FutureConverter<V>(                               \
                    ::RCF::AllocateClientParameters<                          \
                        V ,                                                   \
                        A1,A2,A3,A4,A5,A6,A7,A8 ,                             \
                        V,V,V,V,V,V,V >()(                                    \
                            getClientStub() ,                                 \
                            a1,a2,a3,a4,a5,a6,a7,a8 ,                         \
                            V(),V(),V(),V(),V(),V(),V()).r.get(),             \
                    getClientStub(),                                          \
                    id::value,                                                \
                    callOptions.apply(getClientStub()),                       \
                    #func,                                                    \
                    "V8");                                                    \
            }                                                                 \
            const char * getFunctionName(const id &)                          \
            {                                                                 \
                return #func;                                                 \
            }                                                                 \
            const char * getArity(const id &)                                 \
            {                                                                 \
                return "V8";                                                  \
            }                                                                 \
                                                                              \
        private:                                                              \
            template<typename T>                                              \
            void callMethod(                                                  \
                const id &,                                                   \
                ::RCF::RcfSession &session,                                   \
                T &t)                                                         \
            {                                                                 \
                ::RCF::ServerParameters<                                      \
                    V ,                                                       \
                    A1,A2,A3,A4,A5,A6,A7,A8 > &p =                            \
                        ::RCF::AllocateServerParameters<                      \
                            V ,                                               \
                            A1,A2,A3,A4,A5,A6,A7,A8 >()(session);             \
                RCF_UNUSED_VARIABLE(p);                                       \
                t.func(                                                       \
                    p.a1.get(),                                               \
                        p.a2.get(),                                           \
                        p.a3.get(),                                           \
                        p.a4.get(),                                           \
                        p.a5.get(),                                           \
                        p.a6.get(),                                           \
                        p.a7.get(),                                           \
                        p.a8.get());                                          \
            }




//------------------------------------------------------------------------------
// Parameters - R9
//------------------------------------------------------------------------------

// RCF_METHOD_R9
#define RCF_METHOD_R9(R,func , A1,A2,A3,A4,A5,A6,A7,A8,A9)                    \
    RCF_METHOD_R9_(R,func , A1,A2,A3,A4,A5,A6,A7,A8,A9, RCF_MAKE_UNIQUE_ID(func, R9))

#define RCF_METHOD_R9_(R,func , A1,A2,A3,A4,A5,A6,A7,A8,A9, id)               \
        public:                                                               \
            RCF_MAKE_NEXT_DISPATCH_ID(id)                                     \
            ::RCF::FutureConverter<R > func(                                  \
                ::RCF::RemoveOut<A1 >::type a1,                               \
                ::RCF::RemoveOut<A2 >::type a2,                               \
                ::RCF::RemoveOut<A3 >::type a3,                               \
                ::RCF::RemoveOut<A4 >::type a4,                               \
                ::RCF::RemoveOut<A5 >::type a5,                               \
                ::RCF::RemoveOut<A6 >::type a6,                               \
                ::RCF::RemoveOut<A7 >::type a7,                               \
                ::RCF::RemoveOut<A8 >::type a8,                               \
                ::RCF::RemoveOut<A9 >::type a9)                               \
            {                                                                 \
                return func(                                                  \
                    ::RCF::CallOptions() ,                                    \
                    a1,a2,a3,a4,a5,a6,a7,a8,a9);                              \
            }                                                                 \
            ::RCF::FutureConverter<R > func(                                  \
                const ::RCF::CallOptions &callOptions ,                       \
                ::RCF::RemoveOut<A1 >::type a1,                               \
                ::RCF::RemoveOut<A2 >::type a2,                               \
                ::RCF::RemoveOut<A3 >::type a3,                               \
                ::RCF::RemoveOut<A4 >::type a4,                               \
                ::RCF::RemoveOut<A5 >::type a5,                               \
                ::RCF::RemoveOut<A6 >::type a6,                               \
                ::RCF::RemoveOut<A7 >::type a7,                               \
                ::RCF::RemoveOut<A8 >::type a8,                               \
                ::RCF::RemoveOut<A9 >::type a9)                               \
            {                                                                 \
                checkClientInitialized();                                     \
                getClientStub().setAsync(false);                              \
                return RCF::FutureConverter<R >(                              \
                    ::RCF::AllocateClientParameters<                          \
                        R ,                                                   \
                        A1,A2,A3,A4,A5,A6,A7,A8,A9 ,                          \
                        V,V,V,V,V,V >()(                                      \
                            getClientStub() ,                                 \
                            a1,a2,a3,a4,a5,a6,a7,a8,a9 ,                      \
                            V(),V(),V(),V(),V(),V()).r.get(),                 \
                    getClientStub(),                                          \
                    id::value,                                                \
                    callOptions.apply(getClientStub()),                       \
                    #func,                                                    \
                    "R9");                                                    \
            }                                                                 \
            const char * getFunctionName(const id &)                          \
            {                                                                 \
                return #func;                                                 \
            }                                                                 \
            const char * getArity(const id &)                                 \
            {                                                                 \
                return "R9";                                                  \
            }                                                                 \
                                                                              \
        private:                                                              \
            template<typename T>                                              \
            void callMethod(                                                  \
                const id &,                                                   \
                ::RCF::RcfSession &session,                                   \
                T &t)                                                         \
            {                                                                 \
                ::RCF::ServerParameters<                                      \
                    R ,                                                       \
                    A1,A2,A3,A4,A5,A6,A7,A8,A9 > &p =                         \
                    ::RCF::AllocateServerParameters<                          \
                        R ,                                                   \
                        A1,A2,A3,A4,A5,A6,A7,A8,A9 >()(session);              \
                p.r.set(                                                      \
                    session.getAutoSend(),                                    \
                    t.func(                                                   \
                        p.a1.get(),                                           \
                        p.a2.get(),                                           \
                        p.a3.get(),                                           \
                        p.a4.get(),                                           \
                        p.a5.get(),                                           \
                        p.a6.get(),                                           \
                        p.a7.get(),                                           \
                        p.a8.get(),                                           \
                        p.a9.get()));                                         \
            }




//------------------------------------------------------------------------------
// Parameters - V9
//------------------------------------------------------------------------------

// RCF_METHOD_V9
#define RCF_METHOD_V9(R,func , A1,A2,A3,A4,A5,A6,A7,A8,A9)                    \
    RCF_METHOD_V9_(R,func  , A1,A2,A3,A4,A5,A6,A7,A8,A9, RCF_MAKE_UNIQUE_ID(func, V9))

#define RCF_METHOD_V9_(R,func , A1,A2,A3,A4,A5,A6,A7,A8,A9, id)               \
        public:                                                               \
            static_assert( std::is_same<R, void>::value, "RCF_METHOD_Vn() must be used for methods returning void." );\
            RCF_MAKE_NEXT_DISPATCH_ID(id)                                     \
            ::RCF::FutureConverter<V> func(                                   \
                ::RCF::RemoveOut<A1 >::type a1,                               \
                ::RCF::RemoveOut<A2 >::type a2,                               \
                ::RCF::RemoveOut<A3 >::type a3,                               \
                ::RCF::RemoveOut<A4 >::type a4,                               \
                ::RCF::RemoveOut<A5 >::type a5,                               \
                ::RCF::RemoveOut<A6 >::type a6,                               \
                ::RCF::RemoveOut<A7 >::type a7,                               \
                ::RCF::RemoveOut<A8 >::type a8,                               \
                ::RCF::RemoveOut<A9 >::type a9)                               \
            {                                                                 \
                return func(                                                  \
                    ::RCF::CallOptions() ,                                    \
                    a1,a2,a3,a4,a5,a6,a7,a8,a9);                              \
            }                                                                 \
            ::RCF::FutureConverter<V> func(                                   \
                const ::RCF::CallOptions &callOptions ,                       \
                ::RCF::RemoveOut<A1 >::type a1,                               \
                ::RCF::RemoveOut<A2 >::type a2,                               \
                ::RCF::RemoveOut<A3 >::type a3,                               \
                ::RCF::RemoveOut<A4 >::type a4,                               \
                ::RCF::RemoveOut<A5 >::type a5,                               \
                ::RCF::RemoveOut<A6 >::type a6,                               \
                ::RCF::RemoveOut<A7 >::type a7,                               \
                ::RCF::RemoveOut<A8 >::type a8,                               \
                ::RCF::RemoveOut<A9 >::type a9)                               \
            {                                                                 \
                checkClientInitialized();                                     \
                getClientStub().setAsync(false);                              \
                return RCF::FutureConverter<V>(                               \
                    ::RCF::AllocateClientParameters<                          \
                        V ,                                                   \
                        A1,A2,A3,A4,A5,A6,A7,A8,A9 ,                          \
                        V,V,V,V,V,V >()(                                      \
                            getClientStub() ,                                 \
                            a1,a2,a3,a4,a5,a6,a7,a8,a9 ,                      \
                            V(),V(),V(),V(),V(),V()).r.get(),                 \
                    getClientStub(),                                          \
                    id::value,                                                \
                    callOptions.apply(getClientStub()),                       \
                    #func,                                                    \
                    "V9");                                                    \
            }                                                                 \
            const char * getFunctionName(const id &)                          \
            {                                                                 \
                return #func;                                                 \
            }                                                                 \
            const char * getArity(const id &)                                 \
            {                                                                 \
                return "V9";                                                  \
            }                                                                 \
                                                                              \
        private:                                                              \
            template<typename T>                                              \
            void callMethod(                                                  \
                const id &,                                                   \
                ::RCF::RcfSession &session,                                   \
                T &t)                                                         \
            {                                                                 \
                ::RCF::ServerParameters<                                      \
                    V ,                                                       \
                    A1,A2,A3,A4,A5,A6,A7,A8,A9 > &p =                         \
                        ::RCF::AllocateServerParameters<                      \
                            V ,                                               \
                            A1,A2,A3,A4,A5,A6,A7,A8,A9 >()(session);          \
                RCF_UNUSED_VARIABLE(p);                                       \
                t.func(                                                       \
                    p.a1.get(),                                               \
                        p.a2.get(),                                           \
                        p.a3.get(),                                           \
                        p.a4.get(),                                           \
                        p.a5.get(),                                           \
                        p.a6.get(),                                           \
                        p.a7.get(),                                           \
                        p.a8.get(),                                           \
                        p.a9.get());                                          \
            }




//------------------------------------------------------------------------------
// Parameters - R10
//------------------------------------------------------------------------------

// RCF_METHOD_R10
#define RCF_METHOD_R10(R,func , A1,A2,A3,A4,A5,A6,A7,A8,A9,A10)               \
    RCF_METHOD_R10_(R,func , A1,A2,A3,A4,A5,A6,A7,A8,A9,A10, RCF_MAKE_UNIQUE_ID(func, R10))

#define RCF_METHOD_R10_(R,func , A1,A2,A3,A4,A5,A6,A7,A8,A9,A10, id)          \
        public:                                                               \
            RCF_MAKE_NEXT_DISPATCH_ID(id)                                     \
            ::RCF::FutureConverter<R > func(                                  \
                ::RCF::RemoveOut<A1 >::type a1,                               \
                ::RCF::RemoveOut<A2 >::type a2,                               \
                ::RCF::RemoveOut<A3 >::type a3,                               \
                ::RCF::RemoveOut<A4 >::type a4,                               \
                ::RCF::RemoveOut<A5 >::type a5,                               \
                ::RCF::RemoveOut<A6 >::type a6,                               \
                ::RCF::RemoveOut<A7 >::type a7,                               \
                ::RCF::RemoveOut<A8 >::type a8,                               \
                ::RCF::RemoveOut<A9 >::type a9,                               \
                ::RCF::RemoveOut<A10 >::type a10)                             \
            {                                                                 \
                return func(                                                  \
                    ::RCF::CallOptions() ,                                    \
                    a1,a2,a3,a4,a5,a6,a7,a8,a9,a10);                          \
            }                                                                 \
            ::RCF::FutureConverter<R > func(                                  \
                const ::RCF::CallOptions &callOptions ,                       \
                ::RCF::RemoveOut<A1 >::type a1,                               \
                ::RCF::RemoveOut<A2 >::type a2,                               \
                ::RCF::RemoveOut<A3 >::type a3,                               \
                ::RCF::RemoveOut<A4 >::type a4,                               \
                ::RCF::RemoveOut<A5 >::type a5,                               \
                ::RCF::RemoveOut<A6 >::type a6,                               \
                ::RCF::RemoveOut<A7 >::type a7,                               \
                ::RCF::RemoveOut<A8 >::type a8,                               \
                ::RCF::RemoveOut<A9 >::type a9,                               \
                ::RCF::RemoveOut<A10 >::type a10)                             \
            {                                                                 \
                checkClientInitialized();                                     \
                getClientStub().setAsync(false);                              \
                return RCF::FutureConverter<R >(                              \
                    ::RCF::AllocateClientParameters<                          \
                        R ,                                                   \
                        A1,A2,A3,A4,A5,A6,A7,A8,A9,A10 ,                      \
                        V,V,V,V,V >()(                                        \
                            getClientStub() ,                                 \
                            a1,a2,a3,a4,a5,a6,a7,a8,a9,a10 ,                  \
                            V(),V(),V(),V(),V()).r.get(),                     \
                    getClientStub(),                                          \
                    id::value,                                                \
                    callOptions.apply(getClientStub()),                       \
                    #func,                                                    \
                    "R10");                                                   \
            }                                                                 \
            const char * getFunctionName(const id &)                          \
            {                                                                 \
                return #func;                                                 \
            }                                                                 \
            const char * getArity(const id &)                                 \
            {                                                                 \
                return "R10";                                                 \
            }                                                                 \
                                                                              \
        private:                                                              \
            template<typename T>                                              \
            void callMethod(                                                  \
                const id &,                                                   \
                ::RCF::RcfSession &session,                                   \
                T &t)                                                         \
            {                                                                 \
                ::RCF::ServerParameters<                                      \
                    R ,                                                       \
                    A1,A2,A3,A4,A5,A6,A7,A8,A9,A10 > &p =                     \
                    ::RCF::AllocateServerParameters<                          \
                        R ,                                                   \
                        A1,A2,A3,A4,A5,A6,A7,A8,A9,A10 >()(session);          \
                p.r.set(                                                      \
                    session.getAutoSend(),                                    \
                    t.func(                                                   \
                        p.a1.get(),                                           \
                        p.a2.get(),                                           \
                        p.a3.get(),                                           \
                        p.a4.get(),                                           \
                        p.a5.get(),                                           \
                        p.a6.get(),                                           \
                        p.a7.get(),                                           \
                        p.a8.get(),                                           \
                        p.a9.get(),                                           \
                        p.a10.get()));                                        \
            }




//------------------------------------------------------------------------------
// Parameters - V10
//------------------------------------------------------------------------------

// RCF_METHOD_V10
#define RCF_METHOD_V10(R,func , A1,A2,A3,A4,A5,A6,A7,A8,A9,A10)               \
    RCF_METHOD_V10_(R,func  , A1,A2,A3,A4,A5,A6,A7,A8,A9,A10, RCF_MAKE_UNIQUE_ID(func, V10))

#define RCF_METHOD_V10_(R,func , A1,A2,A3,A4,A5,A6,A7,A8,A9,A10, id)          \
        public:                                                               \
            static_assert( std::is_same<R, void>::value, "RCF_METHOD_Vn() must be used for methods returning void." );\
            RCF_MAKE_NEXT_DISPATCH_ID(id)                                     \
            ::RCF::FutureConverter<V> func(                                   \
                ::RCF::RemoveOut<A1 >::type a1,                               \
                ::RCF::RemoveOut<A2 >::type a2,                               \
                ::RCF::RemoveOut<A3 >::type a3,                               \
                ::RCF::RemoveOut<A4 >::type a4,                               \
                ::RCF::RemoveOut<A5 >::type a5,                               \
                ::RCF::RemoveOut<A6 >::type a6,                               \
                ::RCF::RemoveOut<A7 >::type a7,                               \
                ::RCF::RemoveOut<A8 >::type a8,                               \
                ::RCF::RemoveOut<A9 >::type a9,                               \
                ::RCF::RemoveOut<A10 >::type a10)                             \
            {                                                                 \
                return func(                                                  \
                    ::RCF::CallOptions() ,                                    \
                    a1,a2,a3,a4,a5,a6,a7,a8,a9,a10);                          \
            }                                                                 \
            ::RCF::FutureConverter<V> func(                                   \
                const ::RCF::CallOptions &callOptions ,                       \
                ::RCF::RemoveOut<A1 >::type a1,                               \
                ::RCF::RemoveOut<A2 >::type a2,                               \
                ::RCF::RemoveOut<A3 >::type a3,                               \
                ::RCF::RemoveOut<A4 >::type a4,                               \
                ::RCF::RemoveOut<A5 >::type a5,                               \
                ::RCF::RemoveOut<A6 >::type a6,                               \
                ::RCF::RemoveOut<A7 >::type a7,                               \
                ::RCF::RemoveOut<A8 >::type a8,                               \
                ::RCF::RemoveOut<A9 >::type a9,                               \
                ::RCF::RemoveOut<A10 >::type a10)                             \
            {                                                                 \
                checkClientInitialized();                                     \
                getClientStub().setAsync(false);                              \
                return RCF::FutureConverter<V>(                               \
                    ::RCF::AllocateClientParameters<                          \
                        V ,                                                   \
                        A1,A2,A3,A4,A5,A6,A7,A8,A9,A10 ,                      \
                        V,V,V,V,V >()(                                        \
                            getClientStub() ,                                 \
                            a1,a2,a3,a4,a5,a6,a7,a8,a9,a10 ,                  \
                            V(),V(),V(),V(),V()).r.get(),                     \
                    getClientStub(),                                          \
                    id::value,                                                \
                    callOptions.apply(getClientStub()),                       \
                    #func,                                                    \
                    "V10");                                                   \
            }                                                                 \
            const char * getFunctionName(const id &)                          \
            {                                                                 \
                return #func;                                                 \
            }                                                                 \
            const char * getArity(const id &)                                 \
            {                                                                 \
                return "V10";                                                 \
            }                                                                 \
                                                                              \
        private:                                                              \
            template<typename T>                                              \
            void callMethod(                                                  \
                const id &,                                                   \
                ::RCF::RcfSession &session,                                   \
                T &t)                                                         \
            {                                                                 \
                ::RCF::ServerParameters<                                      \
                    V ,                                                       \
                    A1,A2,A3,A4,A5,A6,A7,A8,A9,A10 > &p =                     \
                        ::RCF::AllocateServerParameters<                      \
                            V ,                                               \
                            A1,A2,A3,A4,A5,A6,A7,A8,A9,A10 >()(session);      \
                RCF_UNUSED_VARIABLE(p);                                       \
                t.func(                                                       \
                    p.a1.get(),                                               \
                        p.a2.get(),                                           \
                        p.a3.get(),                                           \
                        p.a4.get(),                                           \
                        p.a5.get(),                                           \
                        p.a6.get(),                                           \
                        p.a7.get(),                                           \
                        p.a8.get(),                                           \
                        p.a9.get(),                                           \
                        p.a10.get());                                         \
            }




//------------------------------------------------------------------------------
// Parameters - R11
//------------------------------------------------------------------------------

// RCF_METHOD_R11
#define RCF_METHOD_R11(R,func , A1,A2,A3,A4,A5,A6,A7,A8,A9,A10,A11)           \
    RCF_METHOD_R11_(R,func , A1,A2,A3,A4,A5,A6,A7,A8,A9,A10,A11, RCF_MAKE_UNIQUE_ID(func, R11))

#define RCF_METHOD_R11_(R,func , A1,A2,A3,A4,A5,A6,A7,A8,A9,A10,A11, id)      \
        public:                                                               \
            RCF_MAKE_NEXT_DISPATCH_ID(id)                                     \
            ::RCF::FutureConverter<R > func(                                  \
                ::RCF::RemoveOut<A1 >::type a1,                               \
                ::RCF::RemoveOut<A2 >::type a2,                               \
                ::RCF::RemoveOut<A3 >::type a3,                               \
                ::RCF::RemoveOut<A4 >::type a4,                               \
                ::RCF::RemoveOut<A5 >::type a5,                               \
                ::RCF::RemoveOut<A6 >::type a6,                               \
                ::RCF::RemoveOut<A7 >::type a7,                               \
                ::RCF::RemoveOut<A8 >::type a8,                               \
                ::RCF::RemoveOut<A9 >::type a9,                               \
                ::RCF::RemoveOut<A10 >::type a10,                             \
                ::RCF::RemoveOut<A11 >::type a11)                             \
            {                                                                 \
                return func(                                                  \
                    ::RCF::CallOptions() ,                                    \
                    a1,a2,a3,a4,a5,a6,a7,a8,a9,a10,a11);                      \
            }                                                                 \
            ::RCF::FutureConverter<R > func(                                  \
                const ::RCF::CallOptions &callOptions ,                       \
                ::RCF::RemoveOut<A1 >::type a1,                               \
                ::RCF::RemoveOut<A2 >::type a2,                               \
                ::RCF::RemoveOut<A3 >::type a3,                               \
                ::RCF::RemoveOut<A4 >::type a4,                               \
                ::RCF::RemoveOut<A5 >::type a5,                               \
                ::RCF::RemoveOut<A6 >::type a6,                               \
                ::RCF::RemoveOut<A7 >::type a7,                               \
                ::RCF::RemoveOut<A8 >::type a8,                               \
                ::RCF::RemoveOut<A9 >::type a9,                               \
                ::RCF::RemoveOut<A10 >::type a10,                             \
                ::RCF::RemoveOut<A11 >::type a11)                             \
            {                                                                 \
                checkClientInitialized();                                     \
                getClientStub().setAsync(false);                              \
                return RCF::FutureConverter<R >(                              \
                    ::RCF::AllocateClientParameters<                          \
                        R ,                                                   \
                        A1,A2,A3,A4,A5,A6,A7,A8,A9,A10,A11 ,                  \
                        V,V,V,V >()(                                          \
                            getClientStub() ,                                 \
                            a1,a2,a3,a4,a5,a6,a7,a8,a9,a10,a11 ,              \
                            V(),V(),V(),V()).r.get(),                         \
                    getClientStub(),                                          \
                    id::value,                                                \
                    callOptions.apply(getClientStub()),                       \
                    #func,                                                    \
                    "R11");                                                   \
            }                                                                 \
            const char * getFunctionName(const id &)                          \
            {                                                                 \
                return #func;                                                 \
            }                                                                 \
            const char * getArity(const id &)                                 \
            {                                                                 \
                return "R11";                                                 \
            }                                                                 \
                                                                              \
        private:                                                              \
            template<typename T>                                              \
            void callMethod(                                                  \
                const id &,                                                   \
                ::RCF::RcfSession &session,                                   \
                T &t)                                                         \
            {                                                                 \
                ::RCF::ServerParameters<                                      \
                    R ,                                                       \
                    A1,A2,A3,A4,A5,A6,A7,A8,A9,A10,A11 > &p =                 \
                    ::RCF::AllocateServerParameters<                          \
                        R ,                                                   \
                        A1,A2,A3,A4,A5,A6,A7,A8,A9,A10,A11 >()(session);      \
                p.r.set(                                                      \
                    session.getAutoSend(),                                    \
                    t.func(                                                   \
                        p.a1.get(),                                           \
                        p.a2.get(),                                           \
                        p.a3.get(),                                           \
                        p.a4.get(),                                           \
                        p.a5.get(),                                           \
                        p.a6.get(),                                           \
                        p.a7.get(),                                           \
                        p.a8.get(),                                           \
                        p.a9.get(),                                           \
                        p.a10.get(),                                          \
                        p.a11.get()));                                        \
            }




//------------------------------------------------------------------------------
// Parameters - V11
//------------------------------------------------------------------------------

// RCF_METHOD_V11
#define RCF_METHOD_V11(R,func , A1,A2,A3,A4,A5,A6,A7,A8,A9,A10,A11)           \
    RCF_METHOD_V11_(R,func  , A1,A2,A3,A4,A5,A6,A7,A8,A9,A10,A11, RCF_MAKE_UNIQUE_ID(func, V11))

#define RCF_METHOD_V11_(R,func , A1,A2,A3,A4,A5,A6,A7,A8,A9,A10,A11, id)      \
        public:                                                               \
            static_assert( std::is_same<R, void>::value, "RCF_METHOD_Vn() must be used for methods returning void." );\
            RCF_MAKE_NEXT_DISPATCH_ID(id)                                     \
            ::RCF::FutureConverter<V> func(                                   \
                ::RCF::RemoveOut<A1 >::type a1,                               \
                ::RCF::RemoveOut<A2 >::type a2,                               \
                ::RCF::RemoveOut<A3 >::type a3,                               \
                ::RCF::RemoveOut<A4 >::type a4,                               \
                ::RCF::RemoveOut<A5 >::type a5,                               \
                ::RCF::RemoveOut<A6 >::type a6,                               \
                ::RCF::RemoveOut<A7 >::type a7,                               \
                ::RCF::RemoveOut<A8 >::type a8,                               \
                ::RCF::RemoveOut<A9 >::type a9,                               \
                ::RCF::RemoveOut<A10 >::type a10,                             \
                ::RCF::RemoveOut<A11 >::type a11)                             \
            {                                                                 \
                return func(                                                  \
                    ::RCF::CallOptions() ,                                    \
                    a1,a2,a3,a4,a5,a6,a7,a8,a9,a10,a11);                      \
            }                                                                 \
            ::RCF::FutureConverter<V> func(                                   \
                const ::RCF::CallOptions &callOptions ,                       \
                ::RCF::RemoveOut<A1 >::type a1,                               \
                ::RCF::RemoveOut<A2 >::type a2,                               \
                ::RCF::RemoveOut<A3 >::type a3,                               \
                ::RCF::RemoveOut<A4 >::type a4,                               \
                ::RCF::RemoveOut<A5 >::type a5,                               \
                ::RCF::RemoveOut<A6 >::type a6,                               \
                ::RCF::RemoveOut<A7 >::type a7,                               \
                ::RCF::RemoveOut<A8 >::type a8,                               \
                ::RCF::RemoveOut<A9 >::type a9,                               \
                ::RCF::RemoveOut<A10 >::type a10,                             \
                ::RCF::RemoveOut<A11 >::type a11)                             \
            {                                                                 \
                checkClientInitialized();                                     \
                getClientStub().setAsync(false);                              \
                return RCF::FutureConverter<V>(                               \
                    ::RCF::AllocateClientParameters<                          \
                        V ,                                                   \
                        A1,A2,A3,A4,A5,A6,A7,A8,A9,A10,A11 ,                  \
                        V,V,V,V >()(                                          \
                            getClientStub() ,                                 \
                            a1,a2,a3,a4,a5,a6,a7,a8,a9,a10,a11 ,              \
                            V(),V(),V(),V()).r.get(),                         \
                    getClientStub(),                                          \
                    id::value,                                                \
                    callOptions.apply(getClientStub()),                       \
                    #func,                                                    \
                    "V11");                                                   \
            }                                                                 \
            const char * getFunctionName(const id &)                          \
            {                                                                 \
                return #func;                                                 \
            }                                                                 \
            const char * getArity(const id &)                                 \
            {                                                                 \
                return "V11";                                                 \
            }                                                                 \
                                                                              \
        private:                                                              \
            template<typename T>                                              \
            void callMethod(                                                  \
                const id &,                                                   \
                ::RCF::RcfSession &session,                                   \
                T &t)                                                         \
            {                                                                 \
                ::RCF::ServerParameters<                                      \
                    V ,                                                       \
                    A1,A2,A3,A4,A5,A6,A7,A8,A9,A10,A11 > &p =                 \
                        ::RCF::AllocateServerParameters<                      \
                            V ,                                               \
                            A1,A2,A3,A4,A5,A6,A7,A8,A9,A10,A11 >()(session);  \
                RCF_UNUSED_VARIABLE(p);                                       \
                t.func(                                                       \
                    p.a1.get(),                                               \
                        p.a2.get(),                                           \
                        p.a3.get(),                                           \
                        p.a4.get(),                                           \
                        p.a5.get(),                                           \
                        p.a6.get(),                                           \
                        p.a7.get(),                                           \
                        p.a8.get(),                                           \
                        p.a9.get(),                                           \
                        p.a10.get(),                                          \
                        p.a11.get());                                         \
            }




//------------------------------------------------------------------------------
// Parameters - R12
//------------------------------------------------------------------------------

// RCF_METHOD_R12
#define RCF_METHOD_R12(R,func , A1,A2,A3,A4,A5,A6,A7,A8,A9,A10,A11,A12)       \
    RCF_METHOD_R12_(R,func , A1,A2,A3,A4,A5,A6,A7,A8,A9,A10,A11,A12, RCF_MAKE_UNIQUE_ID(func, R12))

#define RCF_METHOD_R12_(R,func , A1,A2,A3,A4,A5,A6,A7,A8,A9,A10,A11,A12, id)  \
        public:                                                               \
            RCF_MAKE_NEXT_DISPATCH_ID(id)                                     \
            ::RCF::FutureConverter<R > func(                                  \
                ::RCF::RemoveOut<A1 >::type a1,                               \
                ::RCF::RemoveOut<A2 >::type a2,                               \
                ::RCF::RemoveOut<A3 >::type a3,                               \
                ::RCF::RemoveOut<A4 >::type a4,                               \
                ::RCF::RemoveOut<A5 >::type a5,                               \
                ::RCF::RemoveOut<A6 >::type a6,                               \
                ::RCF::RemoveOut<A7 >::type a7,                               \
                ::RCF::RemoveOut<A8 >::type a8,                               \
                ::RCF::RemoveOut<A9 >::type a9,                               \
                ::RCF::RemoveOut<A10 >::type a10,                             \
                ::RCF::RemoveOut<A11 >::type a11,                             \
                ::RCF::RemoveOut<A12 >::type a12)                             \
            {                                                                 \
                return func(                                                  \
                    ::RCF::CallOptions() ,                                    \
                    a1,a2,a3,a4,a5,a6,a7,a8,a9,a10,a11,a12);                  \
            }                                                                 \
            ::RCF::FutureConverter<R > func(                                  \
                const ::RCF::CallOptions &callOptions ,                       \
                ::RCF::RemoveOut<A1 >::type a1,                               \
                ::RCF::RemoveOut<A2 >::type a2,                               \
                ::RCF::RemoveOut<A3 >::type a3,                               \
                ::RCF::RemoveOut<A4 >::type a4,                               \
                ::RCF::RemoveOut<A5 >::type a5,                               \
                ::RCF::RemoveOut<A6 >::type a6,                               \
                ::RCF::RemoveOut<A7 >::type a7,                               \
                ::RCF::RemoveOut<A8 >::type a8,                               \
                ::RCF::RemoveOut<A9 >::type a9,                               \
                ::RCF::RemoveOut<A10 >::type a10,                             \
                ::RCF::RemoveOut<A11 >::type a11,                             \
                ::RCF::RemoveOut<A12 >::type a12)                             \
            {                                                                 \
                checkClientInitialized();                                     \
                getClientStub().setAsync(false);                              \
                return RCF::FutureConverter<R >(                              \
                    ::RCF::AllocateClientParameters<                          \
                        R ,                                                   \
                        A1,A2,A3,A4,A5,A6,A7,A8,A9,A10,A11,A12 ,              \
                        V,V,V >()(                                            \
                            getClientStub() ,                                 \
                            a1,a2,a3,a4,a5,a6,a7,a8,a9,a10,a11,a12 ,          \
                            V(),V(),V()).r.get(),                             \
                    getClientStub(),                                          \
                    id::value,                                                \
                    callOptions.apply(getClientStub()),                       \
                    #func,                                                    \
                    "R12");                                                   \
            }                                                                 \
            const char * getFunctionName(const id &)                          \
            {                                                                 \
                return #func;                                                 \
            }                                                                 \
            const char * getArity(const id &)                                 \
            {                                                                 \
                return "R12";                                                 \
            }                                                                 \
                                                                              \
        private:                                                              \
            template<typename T>                                              \
            void callMethod(                                                  \
                const id &,                                                   \
                ::RCF::RcfSession &session,                                   \
                T &t)                                                         \
            {                                                                 \
                ::RCF::ServerParameters<                                      \
                    R ,                                                       \
                    A1,A2,A3,A4,A5,A6,A7,A8,A9,A10,A11,A12 > &p =             \
                    ::RCF::AllocateServerParameters<                          \
                        R ,                                                   \
                        A1,A2,A3,A4,A5,A6,A7,A8,A9,A10,A11,A12 >()(session);  \
                p.r.set(                                                      \
                    session.getAutoSend(),                                    \
                    t.func(                                                   \
                        p.a1.get(),                                           \
                        p.a2.get(),                                           \
                        p.a3.get(),                                           \
                        p.a4.get(),                                           \
                        p.a5.get(),                                           \
                        p.a6.get(),                                           \
                        p.a7.get(),                                           \
                        p.a8.get(),                                           \
                        p.a9.get(),                                           \
                        p.a10.get(),                                          \
                        p.a11.get(),                                          \
                        p.a12.get()));                                        \
            }




//------------------------------------------------------------------------------
// Parameters - V12
//------------------------------------------------------------------------------

// RCF_METHOD_V12
#define RCF_METHOD_V12(R,func , A1,A2,A3,A4,A5,A6,A7,A8,A9,A10,A11,A12)       \
    RCF_METHOD_V12_(R,func  , A1,A2,A3,A4,A5,A6,A7,A8,A9,A10,A11,A12, RCF_MAKE_UNIQUE_ID(func, V12))

#define RCF_METHOD_V12_(R,func , A1,A2,A3,A4,A5,A6,A7,A8,A9,A10,A11,A12, id)  \
        public:                                                               \
            static_assert( std::is_same<R, void>::value, "RCF_METHOD_Vn() must be used for methods returning void." );\
            RCF_MAKE_NEXT_DISPATCH_ID(id)                                     \
            ::RCF::FutureConverter<V> func(                                   \
                ::RCF::RemoveOut<A1 >::type a1,                               \
                ::RCF::RemoveOut<A2 >::type a2,                               \
                ::RCF::RemoveOut<A3 >::type a3,                               \
                ::RCF::RemoveOut<A4 >::type a4,                               \
                ::RCF::RemoveOut<A5 >::type a5,                               \
                ::RCF::RemoveOut<A6 >::type a6,                               \
                ::RCF::RemoveOut<A7 >::type a7,                               \
                ::RCF::RemoveOut<A8 >::type a8,                               \
                ::RCF::RemoveOut<A9 >::type a9,                               \
                ::RCF::RemoveOut<A10 >::type a10,                             \
                ::RCF::RemoveOut<A11 >::type a11,                             \
                ::RCF::RemoveOut<A12 >::type a12)                             \
            {                                                                 \
                return func(                                                  \
                    ::RCF::CallOptions() ,                                    \
                    a1,a2,a3,a4,a5,a6,a7,a8,a9,a10,a11,a12);                  \
            }                                                                 \
            ::RCF::FutureConverter<V> func(                                   \
                const ::RCF::CallOptions &callOptions ,                       \
                ::RCF::RemoveOut<A1 >::type a1,                               \
                ::RCF::RemoveOut<A2 >::type a2,                               \
                ::RCF::RemoveOut<A3 >::type a3,                               \
                ::RCF::RemoveOut<A4 >::type a4,                               \
                ::RCF::RemoveOut<A5 >::type a5,                               \
                ::RCF::RemoveOut<A6 >::type a6,                               \
                ::RCF::RemoveOut<A7 >::type a7,                               \
                ::RCF::RemoveOut<A8 >::type a8,                               \
                ::RCF::RemoveOut<A9 >::type a9,                               \
                ::RCF::RemoveOut<A10 >::type a10,                             \
                ::RCF::RemoveOut<A11 >::type a11,                             \
                ::RCF::RemoveOut<A12 >::type a12)                             \
            {                                                                 \
                checkClientInitialized();                                     \
                getClientStub().setAsync(false);                              \
                return RCF::FutureConverter<V>(                               \
                    ::RCF::AllocateClientParameters<                          \
                        V ,                                                   \
                        A1,A2,A3,A4,A5,A6,A7,A8,A9,A10,A11,A12 ,              \
                        V,V,V >()(                                            \
                            getClientStub() ,                                 \
                            a1,a2,a3,a4,a5,a6,a7,a8,a9,a10,a11,a12 ,          \
                            V(),V(),V()).r.get(),                             \
                    getClientStub(),                                          \
                    id::value,                                                \
                    callOptions.apply(getClientStub()),                       \
                    #func,                                                    \
                    "V12");                                                   \
            }                                                                 \
            const char * getFunctionName(const id &)                          \
            {                                                                 \
                return #func;                                                 \
            }                                                                 \
            const char * getArity(const id &)                                 \
            {                                                                 \
                return "V12";                                                 \
            }                                                                 \
                                                                              \
        private:                                                              \
            template<typename T>                                              \
            void callMethod(                                                  \
                const id &,                                                   \
                ::RCF::RcfSession &session,                                   \
                T &t)                                                         \
            {                                                                 \
                ::RCF::ServerParameters<                                      \
                    V ,                                                       \
                    A1,A2,A3,A4,A5,A6,A7,A8,A9,A10,A11,A12 > &p =             \
                        ::RCF::AllocateServerParameters<                      \
                            V ,                                               \
                            A1,A2,A3,A4,A5,A6,A7,A8,A9,A10,A11,A12 >()(session);\
                RCF_UNUSED_VARIABLE(p);                                       \
                t.func(                                                       \
                    p.a1.get(),                                               \
                        p.a2.get(),                                           \
                        p.a3.get(),                                           \
                        p.a4.get(),                                           \
                        p.a5.get(),                                           \
                        p.a6.get(),                                           \
                        p.a7.get(),                                           \
                        p.a8.get(),                                           \
                        p.a9.get(),                                           \
                        p.a10.get(),                                          \
                        p.a11.get(),                                          \
                        p.a12.get());                                         \
            }




//------------------------------------------------------------------------------
// Parameters - R13
//------------------------------------------------------------------------------

// RCF_METHOD_R13
#define RCF_METHOD_R13(R,func , A1,A2,A3,A4,A5,A6,A7,A8,A9,A10,A11,A12,A13)   \
    RCF_METHOD_R13_(R,func , A1,A2,A3,A4,A5,A6,A7,A8,A9,A10,A11,A12,A13, RCF_MAKE_UNIQUE_ID(func, R13))

#define RCF_METHOD_R13_(R,func , A1,A2,A3,A4,A5,A6,A7,A8,A9,A10,A11,A12,A13, id)\
        public:                                                               \
            RCF_MAKE_NEXT_DISPATCH_ID(id)                                     \
            ::RCF::FutureConverter<R > func(                                  \
                ::RCF::RemoveOut<A1 >::type a1,                               \
                ::RCF::RemoveOut<A2 >::type a2,                               \
                ::RCF::RemoveOut<A3 >::type a3,                               \
                ::RCF::RemoveOut<A4 >::type a4,                               \
                ::RCF::RemoveOut<A5 >::type a5,                               \
                ::RCF::RemoveOut<A6 >::type a6,                               \
                ::RCF::RemoveOut<A7 >::type a7,                               \
                ::RCF::RemoveOut<A8 >::type a8,                               \
                ::RCF::RemoveOut<A9 >::type a9,                               \
                ::RCF::RemoveOut<A10 >::type a10,                             \
                ::RCF::RemoveOut<A11 >::type a11,                             \
                ::RCF::RemoveOut<A12 >::type a12,                             \
                ::RCF::RemoveOut<A13 >::type a13)                             \
            {                                                                 \
                return func(                                                  \
                    ::RCF::CallOptions() ,                                    \
                    a1,a2,a3,a4,a5,a6,a7,a8,a9,a10,a11,a12,a13);              \
            }                                                                 \
            ::RCF::FutureConverter<R > func(                                  \
                const ::RCF::CallOptions &callOptions ,                       \
                ::RCF::RemoveOut<A1 >::type a1,                               \
                ::RCF::RemoveOut<A2 >::type a2,                               \
                ::RCF::RemoveOut<A3 >::type a3,                               \
                ::RCF::RemoveOut<A4 >::type a4,                               \
                ::RCF::RemoveOut<A5 >::type a5,                               \
                ::RCF::RemoveOut<A6 >::type a6,                               \
                ::RCF::RemoveOut<A7 >::type a7,                               \
                ::RCF::RemoveOut<A8 >::type a8,                               \
                ::RCF::RemoveOut<A9 >::type a9,                               \
                ::RCF::RemoveOut<A10 >::type a10,                             \
                ::RCF::RemoveOut<A11 >::type a11,                             \
                ::RCF::RemoveOut<A12 >::type a12,                             \
                ::RCF::RemoveOut<A13 >::type a13)                             \
            {                                                                 \
                checkClientInitialized();                                     \
                getClientStub().setAsync(false);                              \
                return RCF::FutureConverter<R >(                              \
                    ::RCF::AllocateClientParameters<                          \
                        R ,                                                   \
                        A1,A2,A3,A4,A5,A6,A7,A8,A9,A10,A11,A12,A13 ,          \
                        V,V >()(                                              \
                            getClientStub() ,                                 \
                            a1,a2,a3,a4,a5,a6,a7,a8,a9,a10,a11,a12,a13 ,      \
                            V(),V()).r.get(),                                 \
                    getClientStub(),                                          \
                    id::value,                                                \
                    callOptions.apply(getClientStub()),                       \
                    #func,                                                    \
                    "R13");                                                   \
            }                                                                 \
            const char * getFunctionName(const id &)                          \
            {                                                                 \
                return #func;                                                 \
            }                                                                 \
            const char * getArity(const id &)                                 \
            {                                                                 \
                return "R13";                                                 \
            }                                                                 \
                                                                              \
        private:                                                              \
            template<typename T>                                              \
            void callMethod(                                                  \
                const id &,                                                   \
                ::RCF::RcfSession &session,                                   \
                T &t)                                                         \
            {                                                                 \
                ::RCF::ServerParameters<                                      \
                    R ,                                                       \
                    A1,A2,A3,A4,A5,A6,A7,A8,A9,A10,A11,A12,A13 > &p =         \
                    ::RCF::AllocateServerParameters<                          \
                        R ,                                                   \
                        A1,A2,A3,A4,A5,A6,A7,A8,A9,A10,A11,A12,A13 >()(session);\
                p.r.set(                                                      \
                    session.getAutoSend(),                                    \
                    t.func(                                                   \
                        p.a1.get(),                                           \
                        p.a2.get(),                                           \
                        p.a3.get(),                                           \
                        p.a4.get(),                                           \
                        p.a5.get(),                                           \
                        p.a6.get(),                                           \
                        p.a7.get(),                                           \
                        p.a8.get(),                                           \
                        p.a9.get(),                                           \
                        p.a10.get(),                                          \
                        p.a11.get(),                                          \
                        p.a12.get(),                                          \
                        p.a13.get()));                                        \
            }




//------------------------------------------------------------------------------
// Parameters - V13
//------------------------------------------------------------------------------

// RCF_METHOD_V13
#define RCF_METHOD_V13(R,func , A1,A2,A3,A4,A5,A6,A7,A8,A9,A10,A11,A12,A13)   \
    RCF_METHOD_V13_(R,func  , A1,A2,A3,A4,A5,A6,A7,A8,A9,A10,A11,A12,A13, RCF_MAKE_UNIQUE_ID(func, V13))

#define RCF_METHOD_V13_(R,func , A1,A2,A3,A4,A5,A6,A7,A8,A9,A10,A11,A12,A13, id)\
        public:                                                               \
            static_assert( std::is_same<R, void>::value, "RCF_METHOD_Vn() must be used for methods returning void." );\
            RCF_MAKE_NEXT_DISPATCH_ID(id)                                     \
            ::RCF::FutureConverter<V> func(                                   \
                ::RCF::RemoveOut<A1 >::type a1,                               \
                ::RCF::RemoveOut<A2 >::type a2,                               \
                ::RCF::RemoveOut<A3 >::type a3,                               \
                ::RCF::RemoveOut<A4 >::type a4,                               \
                ::RCF::RemoveOut<A5 >::type a5,                               \
                ::RCF::RemoveOut<A6 >::type a6,                               \
                ::RCF::RemoveOut<A7 >::type a7,                               \
                ::RCF::RemoveOut<A8 >::type a8,                               \
                ::RCF::RemoveOut<A9 >::type a9,                               \
                ::RCF::RemoveOut<A10 >::type a10,                             \
                ::RCF::RemoveOut<A11 >::type a11,                             \
                ::RCF::RemoveOut<A12 >::type a12,                             \
                ::RCF::RemoveOut<A13 >::type a13)                             \
            {                                                                 \
                return func(                                                  \
                    ::RCF::CallOptions() ,                                    \
                    a1,a2,a3,a4,a5,a6,a7,a8,a9,a10,a11,a12,a13);              \
            }                                                                 \
            ::RCF::FutureConverter<V> func(                                   \
                const ::RCF::CallOptions &callOptions ,                       \
                ::RCF::RemoveOut<A1 >::type a1,                               \
                ::RCF::RemoveOut<A2 >::type a2,                               \
                ::RCF::RemoveOut<A3 >::type a3,                               \
                ::RCF::RemoveOut<A4 >::type a4,                               \
                ::RCF::RemoveOut<A5 >::type a5,                               \
                ::RCF::RemoveOut<A6 >::type a6,                               \
                ::RCF::RemoveOut<A7 >::type a7,                               \
                ::RCF::RemoveOut<A8 >::type a8,                               \
                ::RCF::RemoveOut<A9 >::type a9,                               \
                ::RCF::RemoveOut<A10 >::type a10,                             \
                ::RCF::RemoveOut<A11 >::type a11,                             \
                ::RCF::RemoveOut<A12 >::type a12,                             \
                ::RCF::RemoveOut<A13 >::type a13)                             \
            {                                                                 \
                checkClientInitialized();                                     \
                getClientStub().setAsync(false);                              \
                return RCF::FutureConverter<V>(                               \
                    ::RCF::AllocateClientParameters<                          \
                        V ,                                                   \
                        A1,A2,A3,A4,A5,A6,A7,A8,A9,A10,A11,A12,A13 ,          \
                        V,V >()(                                              \
                            getClientStub() ,                                 \
                            a1,a2,a3,a4,a5,a6,a7,a8,a9,a10,a11,a12,a13 ,      \
                            V(),V()).r.get(),                                 \
                    getClientStub(),                                          \
                    id::value,                                                \
                    callOptions.apply(getClientStub()),                       \
                    #func,                                                    \
                    "V13");                                                   \
            }                                                                 \
            const char * getFunctionName(const id &)                          \
            {                                                                 \
                return #func;                                                 \
            }                                                                 \
            const char * getArity(const id &)                                 \
            {                                                                 \
                return "V13";                                                 \
            }                                                                 \
                                                                              \
        private:                                                              \
            template<typename T>                                              \
            void callMethod(                                                  \
                const id &,                                                   \
                ::RCF::RcfSession &session,                                   \
                T &t)                                                         \
            {                                                                 \
                ::RCF::ServerParameters<                                      \
                    V ,                                                       \
                    A1,A2,A3,A4,A5,A6,A7,A8,A9,A10,A11,A12,A13 > &p =         \
                        ::RCF::AllocateServerParameters<                      \
                            V ,                                               \
                            A1,A2,A3,A4,A5,A6,A7,A8,A9,A10,A11,A12,A13 >()(session);\
                RCF_UNUSED_VARIABLE(p);                                       \
                t.func(                                                       \
                    p.a1.get(),                                               \
                        p.a2.get(),                                           \
                        p.a3.get(),                                           \
                        p.a4.get(),                                           \
                        p.a5.get(),                                           \
                        p.a6.get(),                                           \
                        p.a7.get(),                                           \
                        p.a8.get(),                                           \
                        p.a9.get(),                                           \
                        p.a10.get(),                                          \
                        p.a11.get(),                                          \
                        p.a12.get(),                                          \
                        p.a13.get());                                         \
            }




//------------------------------------------------------------------------------
// Parameters - R14
//------------------------------------------------------------------------------

// RCF_METHOD_R14
#define RCF_METHOD_R14(R,func , A1,A2,A3,A4,A5,A6,A7,A8,A9,A10,A11,A12,A13,A14)\
    RCF_METHOD_R14_(R,func , A1,A2,A3,A4,A5,A6,A7,A8,A9,A10,A11,A12,A13,A14, RCF_MAKE_UNIQUE_ID(func, R14))

#define RCF_METHOD_R14_(R,func , A1,A2,A3,A4,A5,A6,A7,A8,A9,A10,A11,A12,A13,A14, id)\
        public:                                                               \
            RCF_MAKE_NEXT_DISPATCH_ID(id)                                     \
            ::RCF::FutureConverter<R > func(                                  \
                ::RCF::RemoveOut<A1 >::type a1,                               \
                ::RCF::RemoveOut<A2 >::type a2,                               \
                ::RCF::RemoveOut<A3 >::type a3,                               \
                ::RCF::RemoveOut<A4 >::type a4,                               \
                ::RCF::RemoveOut<A5 >::type a5,                               \
                ::RCF::RemoveOut<A6 >::type a6,                               \
                ::RCF::RemoveOut<A7 >::type a7,                               \
                ::RCF::RemoveOut<A8 >::type a8,                               \
                ::RCF::RemoveOut<A9 >::type a9,                               \
                ::RCF::RemoveOut<A10 >::type a10,                             \
                ::RCF::RemoveOut<A11 >::type a11,                             \
                ::RCF::RemoveOut<A12 >::type a12,                             \
                ::RCF::RemoveOut<A13 >::type a13,                             \
                ::RCF::RemoveOut<A14 >::type a14)                             \
            {                                                                 \
                return func(                                                  \
                    ::RCF::CallOptions() ,                                    \
                    a1,a2,a3,a4,a5,a6,a7,a8,a9,a10,a11,a12,a13,a14);          \
            }                                                                 \
            ::RCF::FutureConverter<R > func(                                  \
                const ::RCF::CallOptions &callOptions ,                       \
                ::RCF::RemoveOut<A1 >::type a1,                               \
                ::RCF::RemoveOut<A2 >::type a2,                               \
                ::RCF::RemoveOut<A3 >::type a3,                               \
                ::RCF::RemoveOut<A4 >::type a4,                               \
                ::RCF::RemoveOut<A5 >::type a5,                               \
                ::RCF::RemoveOut<A6 >::type a6,                               \
                ::RCF::RemoveOut<A7 >::type a7,                               \
                ::RCF::RemoveOut<A8 >::type a8,                               \
                ::RCF::RemoveOut<A9 >::type a9,                               \
                ::RCF::RemoveOut<A10 >::type a10,                             \
                ::RCF::RemoveOut<A11 >::type a11,                             \
                ::RCF::RemoveOut<A12 >::type a12,                             \
                ::RCF::RemoveOut<A13 >::type a13,                             \
                ::RCF::RemoveOut<A14 >::type a14)                             \
            {                                                                 \
                checkClientInitialized();                                     \
                getClientStub().setAsync(false);                              \
                return RCF::FutureConverter<R >(                              \
                    ::RCF::AllocateClientParameters<                          \
                        R ,                                                   \
                        A1,A2,A3,A4,A5,A6,A7,A8,A9,A10,A11,A12,A13,A14 ,      \
                        V >()(                                                \
                            getClientStub() ,                                 \
                            a1,a2,a3,a4,a5,a6,a7,a8,a9,a10,a11,a12,a13,a14 ,  \
                            V()).r.get(),                                     \
                    getClientStub(),                                          \
                    id::value,                                                \
                    callOptions.apply(getClientStub()),                       \
                    #func,                                                    \
                    "R14");                                                   \
            }                                                                 \
            const char * getFunctionName(const id &)                          \
            {                                                                 \
                return #func;                                                 \
            }                                                                 \
            const char * getArity(const id &)                                 \
            {                                                                 \
                return "R14";                                                 \
            }                                                                 \
                                                                              \
        private:                                                              \
            template<typename T>                                              \
            void callMethod(                                                  \
                const id &,                                                   \
                ::RCF::RcfSession &session,                                   \
                T &t)                                                         \
            {                                                                 \
                ::RCF::ServerParameters<                                      \
                    R ,                                                       \
                    A1,A2,A3,A4,A5,A6,A7,A8,A9,A10,A11,A12,A13,A14 > &p =     \
                    ::RCF::AllocateServerParameters<                          \
                        R ,                                                   \
                        A1,A2,A3,A4,A5,A6,A7,A8,A9,A10,A11,A12,A13,A14 >()(session);\
                p.r.set(                                                      \
                    session.getAutoSend(),                                    \
                    t.func(                                                   \
                        p.a1.get(),                                           \
                        p.a2.get(),                                           \
                        p.a3.get(),                                           \
                        p.a4.get(),                                           \
                        p.a5.get(),                                           \
                        p.a6.get(),                                           \
                        p.a7.get(),                                           \
                        p.a8.get(),                                           \
                        p.a9.get(),                                           \
                        p.a10.get(),                                          \
                        p.a11.get(),                                          \
                        p.a12.get(),                                          \
                        p.a13.get(),                                          \
                        p.a14.get()));                                        \
            }




//------------------------------------------------------------------------------
// Parameters - V14
//------------------------------------------------------------------------------

// RCF_METHOD_V14
#define RCF_METHOD_V14(R,func , A1,A2,A3,A4,A5,A6,A7,A8,A9,A10,A11,A12,A13,A14)\
    RCF_METHOD_V14_(R,func  , A1,A2,A3,A4,A5,A6,A7,A8,A9,A10,A11,A12,A13,A14, RCF_MAKE_UNIQUE_ID(func, V14))

#define RCF_METHOD_V14_(R,func , A1,A2,A3,A4,A5,A6,A7,A8,A9,A10,A11,A12,A13,A14, id)\
        public:                                                               \
            static_assert( std::is_same<R, void>::value, "RCF_METHOD_Vn() must be used for methods returning void." );\
            RCF_MAKE_NEXT_DISPATCH_ID(id)                                     \
            ::RCF::FutureConverter<V> func(                                   \
                ::RCF::RemoveOut<A1 >::type a1,                               \
                ::RCF::RemoveOut<A2 >::type a2,                               \
                ::RCF::RemoveOut<A3 >::type a3,                               \
                ::RCF::RemoveOut<A4 >::type a4,                               \
                ::RCF::RemoveOut<A5 >::type a5,                               \
                ::RCF::RemoveOut<A6 >::type a6,                               \
                ::RCF::RemoveOut<A7 >::type a7,                               \
                ::RCF::RemoveOut<A8 >::type a8,                               \
                ::RCF::RemoveOut<A9 >::type a9,                               \
                ::RCF::RemoveOut<A10 >::type a10,                             \
                ::RCF::RemoveOut<A11 >::type a11,                             \
                ::RCF::RemoveOut<A12 >::type a12,                             \
                ::RCF::RemoveOut<A13 >::type a13,                             \
                ::RCF::RemoveOut<A14 >::type a14)                             \
            {                                                                 \
                return func(                                                  \
                    ::RCF::CallOptions() ,                                    \
                    a1,a2,a3,a4,a5,a6,a7,a8,a9,a10,a11,a12,a13,a14);          \
            }                                                                 \
            ::RCF::FutureConverter<V> func(                                   \
                const ::RCF::CallOptions &callOptions ,                       \
                ::RCF::RemoveOut<A1 >::type a1,                               \
                ::RCF::RemoveOut<A2 >::type a2,                               \
                ::RCF::RemoveOut<A3 >::type a3,                               \
                ::RCF::RemoveOut<A4 >::type a4,                               \
                ::RCF::RemoveOut<A5 >::type a5,                               \
                ::RCF::RemoveOut<A6 >::type a6,                               \
                ::RCF::RemoveOut<A7 >::type a7,                               \
                ::RCF::RemoveOut<A8 >::type a8,                               \
                ::RCF::RemoveOut<A9 >::type a9,                               \
                ::RCF::RemoveOut<A10 >::type a10,                             \
                ::RCF::RemoveOut<A11 >::type a11,                             \
                ::RCF::RemoveOut<A12 >::type a12,                             \
                ::RCF::RemoveOut<A13 >::type a13,                             \
                ::RCF::RemoveOut<A14 >::type a14)                             \
            {                                                                 \
                checkClientInitialized();                                     \
                getClientStub().setAsync(false);                              \
                return RCF::FutureConverter<V>(                               \
                    ::RCF::AllocateClientParameters<                          \
                        V ,                                                   \
                        A1,A2,A3,A4,A5,A6,A7,A8,A9,A10,A11,A12,A13,A14 ,      \
                        V >()(                                                \
                            getClientStub() ,                                 \
                            a1,a2,a3,a4,a5,a6,a7,a8,a9,a10,a11,a12,a13,a14 ,  \
                            V()).r.get(),                                     \
                    getClientStub(),                                          \
                    id::value,                                                \
                    callOptions.apply(getClientStub()),                       \
                    #func,                                                    \
                    "V14");                                                   \
            }                                                                 \
            const char * getFunctionName(const id &)                          \
            {                                                                 \
                return #func;                                                 \
            }                                                                 \
            const char * getArity(const id &)                                 \
            {                                                                 \
                return "V14";                                                 \
            }                                                                 \
                                                                              \
        private:                                                              \
            template<typename T>                                              \
            void callMethod(                                                  \
                const id &,                                                   \
                ::RCF::RcfSession &session,                                   \
                T &t)                                                         \
            {                                                                 \
                ::RCF::ServerParameters<                                      \
                    V ,                                                       \
                    A1,A2,A3,A4,A5,A6,A7,A8,A9,A10,A11,A12,A13,A14 > &p =     \
                        ::RCF::AllocateServerParameters<                      \
                            V ,                                               \
                            A1,A2,A3,A4,A5,A6,A7,A8,A9,A10,A11,A12,A13,A14 >()(session);\
                RCF_UNUSED_VARIABLE(p);                                       \
                t.func(                                                       \
                    p.a1.get(),                                               \
                        p.a2.get(),                                           \
                        p.a3.get(),                                           \
                        p.a4.get(),                                           \
                        p.a5.get(),                                           \
                        p.a6.get(),                                           \
                        p.a7.get(),                                           \
                        p.a8.get(),                                           \
                        p.a9.get(),                                           \
                        p.a10.get(),                                          \
                        p.a11.get(),                                          \
                        p.a12.get(),                                          \
                        p.a13.get(),                                          \
                        p.a14.get());                                         \
            }




//------------------------------------------------------------------------------
// Parameters - R15
//------------------------------------------------------------------------------

// RCF_METHOD_R15
#define RCF_METHOD_R15(R,func , A1,A2,A3,A4,A5,A6,A7,A8,A9,A10,A11,A12,A13,A14,A15)\
    RCF_METHOD_R15_(R,func , A1,A2,A3,A4,A5,A6,A7,A8,A9,A10,A11,A12,A13,A14,A15, RCF_MAKE_UNIQUE_ID(func, R15))

#define RCF_METHOD_R15_(R,func , A1,A2,A3,A4,A5,A6,A7,A8,A9,A10,A11,A12,A13,A14,A15, id)\
        public:                                                               \
            RCF_MAKE_NEXT_DISPATCH_ID(id)                                     \
            ::RCF::FutureConverter<R > func(                                  \
                ::RCF::RemoveOut<A1 >::type a1,                               \
                ::RCF::RemoveOut<A2 >::type a2,                               \
                ::RCF::RemoveOut<A3 >::type a3,                               \
                ::RCF::RemoveOut<A4 >::type a4,                               \
                ::RCF::RemoveOut<A5 >::type a5,                               \
                ::RCF::RemoveOut<A6 >::type a6,                               \
                ::RCF::RemoveOut<A7 >::type a7,                               \
                ::RCF::RemoveOut<A8 >::type a8,                               \
                ::RCF::RemoveOut<A9 >::type a9,                               \
                ::RCF::RemoveOut<A10 >::type a10,                             \
                ::RCF::RemoveOut<A11 >::type a11,                             \
                ::RCF::RemoveOut<A12 >::type a12,                             \
                ::RCF::RemoveOut<A13 >::type a13,                             \
                ::RCF::RemoveOut<A14 >::type a14,                             \
                ::RCF::RemoveOut<A15 >::type a15)                             \
            {                                                                 \
                return func(                                                  \
                    ::RCF::CallOptions() ,                                    \
                    a1,a2,a3,a4,a5,a6,a7,a8,a9,a10,a11,a12,a13,a14,a15);      \
            }                                                                 \
            ::RCF::FutureConverter<R > func(                                  \
                const ::RCF::CallOptions &callOptions ,                       \
                ::RCF::RemoveOut<A1 >::type a1,                               \
                ::RCF::RemoveOut<A2 >::type a2,                               \
                ::RCF::RemoveOut<A3 >::type a3,                               \
                ::RCF::RemoveOut<A4 >::type a4,                               \
                ::RCF::RemoveOut<A5 >::type a5,                               \
                ::RCF::RemoveOut<A6 >::type a6,                               \
                ::RCF::RemoveOut<A7 >::type a7,                               \
                ::RCF::RemoveOut<A8 >::type a8,                               \
                ::RCF::RemoveOut<A9 >::type a9,                               \
                ::RCF::RemoveOut<A10 >::type a10,                             \
                ::RCF::RemoveOut<A11 >::type a11,                             \
                ::RCF::RemoveOut<A12 >::type a12,                             \
                ::RCF::RemoveOut<A13 >::type a13,                             \
                ::RCF::RemoveOut<A14 >::type a14,                             \
                ::RCF::RemoveOut<A15 >::type a15)                             \
            {                                                                 \
                checkClientInitialized();                                     \
                getClientStub().setAsync(false);                              \
                return RCF::FutureConverter<R >(                              \
                    ::RCF::AllocateClientParameters<                          \
                        R ,                                                   \
                        A1,A2,A3,A4,A5,A6,A7,A8,A9,A10,A11,A12,A13,A14,A15    \
                         >()(                                                 \
                            getClientStub() ,                                 \
                            a1,a2,a3,a4,a5,a6,a7,a8,a9,a10,a11,a12,a13,a14,a15 \
                            ).r.get(),                                        \
                    getClientStub(),                                          \
                    id::value,                                                \
                    callOptions.apply(getClientStub()),                       \
                    #func,                                                    \
                    "R15");                                                   \
            }                                                                 \
            const char * getFunctionName(const id &)                          \
            {                                                                 \
                return #func;                                                 \
            }                                                                 \
            const char * getArity(const id &)                                 \
            {                                                                 \
                return "R15";                                                 \
            }                                                                 \
                                                                              \
        private:                                                              \
            template<typename T>                                              \
            void callMethod(                                                  \
                const id &,                                                   \
                ::RCF::RcfSession &session,                                   \
                T &t)                                                         \
            {                                                                 \
                ::RCF::ServerParameters<                                      \
                    R ,                                                       \
                    A1,A2,A3,A4,A5,A6,A7,A8,A9,A10,A11,A12,A13,A14,A15 > &p = \
                    ::RCF::AllocateServerParameters<                          \
                        R ,                                                   \
                        A1,A2,A3,A4,A5,A6,A7,A8,A9,A10,A11,A12,A13,A14,A15 >()(session);\
                p.r.set(                                                      \
                    session.getAutoSend(),                                    \
                    t.func(                                                   \
                        p.a1.get(),                                           \
                        p.a2.get(),                                           \
                        p.a3.get(),                                           \
                        p.a4.get(),                                           \
                        p.a5.get(),                                           \
                        p.a6.get(),                                           \
                        p.a7.get(),                                           \
                        p.a8.get(),                                           \
                        p.a9.get(),                                           \
                        p.a10.get(),                                          \
                        p.a11.get(),                                          \
                        p.a12.get(),                                          \
                        p.a13.get(),                                          \
                        p.a14.get(),                                          \
                        p.a15.get()));                                        \
            }




//------------------------------------------------------------------------------
// Parameters - V15
//------------------------------------------------------------------------------

// RCF_METHOD_V15
#define RCF_METHOD_V15(R,func , A1,A2,A3,A4,A5,A6,A7,A8,A9,A10,A11,A12,A13,A14,A15)\
    RCF_METHOD_V15_(R,func  , A1,A2,A3,A4,A5,A6,A7,A8,A9,A10,A11,A12,A13,A14,A15, RCF_MAKE_UNIQUE_ID(func, V15))

#define RCF_METHOD_V15_(R,func , A1,A2,A3,A4,A5,A6,A7,A8,A9,A10,A11,A12,A13,A14,A15, id)\
        public:                                                               \
            static_assert( std::is_same<R, void>::value, "RCF_METHOD_Vn() must be used for methods returning void." );\
            RCF_MAKE_NEXT_DISPATCH_ID(id)                                     \
            ::RCF::FutureConverter<V> func(                                   \
                ::RCF::RemoveOut<A1 >::type a1,                               \
                ::RCF::RemoveOut<A2 >::type a2,                               \
                ::RCF::RemoveOut<A3 >::type a3,                               \
                ::RCF::RemoveOut<A4 >::type a4,                               \
                ::RCF::RemoveOut<A5 >::type a5,                               \
                ::RCF::RemoveOut<A6 >::type a6,                               \
                ::RCF::RemoveOut<A7 >::type a7,                               \
                ::RCF::RemoveOut<A8 >::type a8,                               \
                ::RCF::RemoveOut<A9 >::type a9,                               \
                ::RCF::RemoveOut<A10 >::type a10,                             \
                ::RCF::RemoveOut<A11 >::type a11,                             \
                ::RCF::RemoveOut<A12 >::type a12,                             \
                ::RCF::RemoveOut<A13 >::type a13,                             \
                ::RCF::RemoveOut<A14 >::type a14,                             \
                ::RCF::RemoveOut<A15 >::type a15)                             \
            {                                                                 \
                return func(                                                  \
                    ::RCF::CallOptions() ,                                    \
                    a1,a2,a3,a4,a5,a6,a7,a8,a9,a10,a11,a12,a13,a14,a15);      \
            }                                                                 \
            ::RCF::FutureConverter<V> func(                                   \
                const ::RCF::CallOptions &callOptions ,                       \
                ::RCF::RemoveOut<A1 >::type a1,                               \
                ::RCF::RemoveOut<A2 >::type a2,                               \
                ::RCF::RemoveOut<A3 >::type a3,                               \
                ::RCF::RemoveOut<A4 >::type a4,                               \
                ::RCF::RemoveOut<A5 >::type a5,                               \
                ::RCF::RemoveOut<A6 >::type a6,                               \
                ::RCF::RemoveOut<A7 >::type a7,                               \
                ::RCF::RemoveOut<A8 >::type a8,                               \
                ::RCF::RemoveOut<A9 >::type a9,                               \
                ::RCF::RemoveOut<A10 >::type a10,                             \
                ::RCF::RemoveOut<A11 >::type a11,                             \
                ::RCF::RemoveOut<A12 >::type a12,                             \
                ::RCF::RemoveOut<A13 >::type a13,                             \
                ::RCF::RemoveOut<A14 >::type a14,                             \
                ::RCF::RemoveOut<A15 >::type a15)                             \
            {                                                                 \
                checkClientInitialized();                                     \
                getClientStub().setAsync(false);                              \
                return RCF::FutureConverter<V>(                               \
                    ::RCF::AllocateClientParameters<                          \
                        V ,                                                   \
                        A1,A2,A3,A4,A5,A6,A7,A8,A9,A10,A11,A12,A13,A14,A15    \
                         >()(                                                 \
                            getClientStub() ,                                 \
                            a1,a2,a3,a4,a5,a6,a7,a8,a9,a10,a11,a12,a13,a14,a15 \
                            ).r.get(),                                        \
                    getClientStub(),                                          \
                    id::value,                                                \
                    callOptions.apply(getClientStub()),                       \
                    #func,                                                    \
                    "V15");                                                   \
            }                                                                 \
            const char * getFunctionName(const id &)                          \
            {                                                                 \
                return #func;                                                 \
            }                                                                 \
            const char * getArity(const id &)                                 \
            {                                                                 \
                return "V15";                                                 \
            }                                                                 \
                                                                              \
        private:                                                              \
            template<typename T>                                              \
            void callMethod(                                                  \
                const id &,                                                   \
                ::RCF::RcfSession &session,                                   \
                T &t)                                                         \
            {                                                                 \
                ::RCF::ServerParameters<                                      \
                    V ,                                                       \
                    A1,A2,A3,A4,A5,A6,A7,A8,A9,A10,A11,A12,A13,A14,A15 > &p = \
                        ::RCF::AllocateServerParameters<                      \
                            V ,                                               \
                            A1,A2,A3,A4,A5,A6,A7,A8,A9,A10,A11,A12,A13,A14,A15 >()(session);\
                RCF_UNUSED_VARIABLE(p);                                       \
                t.func(                                                       \
                    p.a1.get(),                                               \
                        p.a2.get(),                                           \
                        p.a3.get(),                                           \
                        p.a4.get(),                                           \
                        p.a5.get(),                                           \
                        p.a6.get(),                                           \
                        p.a7.get(),                                           \
                        p.a8.get(),                                           \
                        p.a9.get(),                                           \
                        p.a10.get(),                                          \
                        p.a11.get(),                                          \
                        p.a12.get(),                                          \
                        p.a13.get(),                                          \
                        p.a14.get(),                                          \
                        p.a15.get());                                         \
            }

#endif // ! INCLUDE_RCF_RCFMETHODGEN_HPP