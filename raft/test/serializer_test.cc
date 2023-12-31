#include "serializer.h"

#include <cstdlib>
#include <cstring>
#include <iostream>
#include <random>

#include "RCF/ClientStub.hpp"
#include "RCF/Future.hpp"
#include "RCF/InitDeinit.hpp"
#include "RCF/RCF.hpp"
#include "RCF/ThreadLibrary.hpp"
#include "RCF/UdpEndpoint.hpp"
#include "log_entry.h"
#include "raft_struct.h"
#include "raft_type.h"
#include "gtest/gtest.h"

// Register RPC call function
RCF_BEGIN(I_EchoService, "I_EchoService")
RCF_METHOD_R1(RCF::ByteBuffer, Echo, const RCF::ByteBuffer &)
RCF_END()

namespace raft {

class EchoService {
public:
  RCF::ByteBuffer Echo(const RCF::ByteBuffer &receive) { return receive; }
};

class SerializerTest : public ::testing::Test {
public:
  void LaunchServerThread() {
    auto server_work = [this]() {
      RCF::RcfInit rcfInit;
      RCF::RcfServer server(RCF::TcpEndpoint(kLocalTestIp, kLocalTestPort));
      EchoService echoServrice;
      server.getServerTransport().setMaxIncomingMessageLength(100 * 1024 *
                                                              1024);
      server.bind<I_EchoService>(echoServrice);
      server.start();
      // Wait until this echo service is executed at least once
      std::this_thread::sleep_for(std::chrono::milliseconds(kSleepTime));
      int a;
      std::cin >> a;
    };
    auto thread = std::thread(server_work);
    thread.detach();
    RCF::sleepMs(100);
  }

  template <typename T, typename Cmp>
  void SendClientRequestTest(const T &ent, Cmp &cmp) {
    RCF::RcfInit rcfInit;
    RcfClient<I_EchoService> client(
        RCF::TcpEndpoint(kLocalTestIp, kLocalTestPort));

    auto serializer = Serializer::NewSerializer();

    RCF::ByteBuffer buffer(serializer.getSerializeSize(ent));
    serializer.Serialize(&ent, &buffer);

    RCF::ByteBuffer returned = client.Echo(buffer);
    T parse;
    serializer.Deserialize(&returned, &parse);
    ASSERT_TRUE(cmp(ent, parse));
    std::cout << "[PASS] Test Serialize " << typeid(T).name() << std::endl;
  }

  template <typename T, typename Cmp>
  void SendClientRequestAsyncTest(const T &ent, Cmp &cmp) {
    RCF::RcfInit rcfInit;
    RcfClient<I_EchoService> client(
        RCF::TcpEndpoint(kLocalTestIp, kLocalTestPort));

    auto serializer = Serializer::NewSerializer();
    RCF::ByteBuffer buffer(serializer.getSerializeSize(ent));
    serializer.Serialize(&ent, &buffer);

    // Remote call failed if pass 1s
    client.getClientStub().setRemoteCallTimeoutMs(1000);
    client.getClientStub().getTransport().setMaxOutgoingMessageLength(
        100 * 1024 * 1024);
    client.getClientStub().getTransport().setMaxIncomingMessageLength(
        100 * 1024 * 1024);

    // Construct the call back function
    RCF::Future<RCF::ByteBuffer> ret;
    auto cmp_callback = [=]() { onComplete(ent, cmp, ret); };

    ret = client.Echo(RCF::AsyncTwoway(cmp_callback), buffer);
    // When this function finishes, the 'client' object is freed and async
    // function call that has not been finished will be cancelled. Thus we must
    // sleep for a while so that the call back function is called
    WaitWorkDone();
  }

  template <typename T, typename Cmp>
  void onComplete(const T &ent, Cmp &cmp, RCF::Future<RCF::ByteBuffer> ret) {
    auto ePtr = ret.getAsyncException();
    if (ePtr.get()) {
      std::cerr << "RPC Call failed:" << ePtr->getErrorMessage() << std::endl;
    } else {
      RCF::ByteBuffer returned = *ret;
      T parse;
      Serializer::NewSerializer().Deserialize(&returned, &parse);
      ASSERT_TRUE(cmp(ent, parse));
      std::cout << "[PASS] Test Serialize Async " << typeid(T).name()
                << std::endl;
    }
  }

  auto GenerateRandomSlice(int min_len, int max_len) -> Slice {
    auto rand_size = rand() % (max_len - min_len) + min_len;
    auto rand_data = new char[rand_size];
    for (decltype(rand_size) i = 0; i < rand_size; ++i) {
      rand_data[i] = rand();
    }
    return Slice(rand_data, rand_size);
  }

  auto GenerateRandomChunkInfo() -> ChunkInfo {
    return ChunkInfo{static_cast<raft_encoding_param_t>(rand()),
                     static_cast<raft_index_t>(rand())};
  }

  auto GenerateRandomLogEntry(bool generate_data, raft_entry_type type)
      -> LogEntry {
    LogEntry ent;
    ent.SetTerm(rand());
    ent.SetIndex(rand());
    ent.SetChunkInfo(GenerateRandomChunkInfo());
    ent.SetType(type);
    if (generate_data) {
      switch (ent.Type()) {
      case raft::kNormal:
        ent.SetCommandData(GenerateRandomSlice(kMaxDataSize / 2, kMaxDataSize));
        break;
      case raft::kFragments:
        ent.SetNotEncodedSlice(
            GenerateRandomSlice(kMaxDataSize / 2, kMaxDataSize));
        ent.SetFragmentSlice(
            GenerateRandomSlice(kMaxDataSize / 2, kMaxDataSize));
        break;
      case raft::kTypeMax:
        assert(false);
      }
    }
    return ent;
  }

  void WaitServerExit() {
    std::this_thread::sleep_for(std::chrono::milliseconds(kSleepTime * 2));
  }

  void WaitWorkDone() {
    std::this_thread::sleep_for(std::chrono::milliseconds(1000));
  }

  void TestNoDataLogEntryTransfer(bool async);
  void TestCompleteCommandDataLogEntryTransfer(bool async);
  void TestFragmentDataLogEntryTransfer(bool async);
  void TestSerializeRequestVoteArgs(bool async);
  void TestSerializeRequestVoteReply(bool async);
  void TestSerializeAppendEntriesArgs(bool async);
  void TestSerializeAppendEntriesReply(bool async);
  void TestSerializeRequestFragmentsArgs(bool async);
  void TestSerializeRequestFragmentsReply(bool async);

private:
  const std::string kLocalTestIp = "127.0.0.1";
  const int kLocalTestPort = 50001;
  const int kMaxDataSize = 10 * 1024 * 1024;
  const int kSleepTime = 10000;
};

void SerializerTest::TestNoDataLogEntryTransfer(bool async) {
  LogEntry ent = GenerateRandomLogEntry(false, kNormal);
  auto cmp = [](const LogEntry &a, const LogEntry &b) -> bool {
    return a == b;
  };
  if (async) {
    SendClientRequestAsyncTest<LogEntry>(ent, cmp);
  } else {
    SendClientRequestTest<LogEntry>(ent, cmp);
  }
}

void SerializerTest::TestCompleteCommandDataLogEntryTransfer(bool async) {
  LogEntry ent = GenerateRandomLogEntry(true, kNormal);
  auto cmp = [](const LogEntry &a, const LogEntry &b) -> bool {
    return a == b;
  };
  if (async) {
    SendClientRequestAsyncTest<LogEntry>(ent, cmp);
  } else {
    SendClientRequestTest<LogEntry>(ent, cmp);
  }
}

void SerializerTest::TestFragmentDataLogEntryTransfer(bool async) {
  LogEntry ent = GenerateRandomLogEntry(true, kFragments);
  auto cmp = [](const LogEntry &a, const LogEntry &b) -> bool {
    return a == b;
  };
  if (async) {
    SendClientRequestAsyncTest<LogEntry>(ent, cmp);
  } else {
    SendClientRequestTest<LogEntry>(ent, cmp);
  }
}

void SerializerTest::TestSerializeRequestVoteArgs(bool async) {
  RequestVoteArgs args = RequestVoteArgs{
      static_cast<raft_term_t>(rand()), static_cast<raft_node_id_t>(rand()),
      static_cast<raft_index_t>(rand()), static_cast<raft_term_t>(rand())};

  auto cmp = [](const RequestVoteArgs &a, const RequestVoteArgs &b) -> bool {
    return std::memcmp(&a, &b, sizeof(RequestVoteArgs)) == 0;
  };
  if (async) {
    SendClientRequestAsyncTest<RequestVoteArgs>(args, cmp);
  } else {
    SendClientRequestTest<RequestVoteArgs>(args, cmp);
  }
}

void SerializerTest::TestSerializeRequestVoteReply(bool async) {
  RequestVoteReply reply =
      RequestVoteReply{static_cast<raft_term_t>(rand()), rand()};
  auto cmp = [](const RequestVoteReply &a, const RequestVoteReply &b) -> bool {
    return std::memcmp(&a, &b, sizeof(RequestVoteReply)) == 0;
  };
  if (async) {
    SendClientRequestAsyncTest<RequestVoteReply>(reply, cmp);
  } else {
    SendClientRequestTest<RequestVoteReply>(reply, cmp);
  }
}

void SerializerTest::TestSerializeAppendEntriesArgs(bool async) {
  AppendEntriesArgs args = AppendEntriesArgs{
      static_cast<raft_term_t>(rand()),
      static_cast<raft_node_id_t>(rand()),
      static_cast<raft_index_t>(rand()),
      static_cast<raft_term_t>(rand()),
      static_cast<raft_encoding_param_t>(rand()),
      static_cast<raft_index_t>(rand()),
      3,
      {GenerateRandomLogEntry(true, kFragments),
       GenerateRandomLogEntry(true, kFragments),
       GenerateRandomLogEntry(true, kNormal)},
  };

  auto cmp = [](const AppendEntriesArgs &l,
                const AppendEntriesArgs &r) -> bool {
    bool hdr_equal =
        l.term == r.term & l.prev_log_index == r.prev_log_index &
        l.prev_log_term == r.prev_log_term & l.leader_id == r.leader_id &
        l.leader_commit == r.leader_commit & l.entry_cnt == r.entry_cnt;
    if (!hdr_equal || l.entries.size() != r.entries.size()) {
      return false;
    }

    for (decltype(l.entries.size()) i = 0; i < l.entries.size(); ++i) {
      if (!(l.entries[i] == r.entries[i])) {
        return false;
      }
    }
    return true;
  };
  if (async) {
    SendClientRequestAsyncTest<AppendEntriesArgs>(args, cmp);
  } else {
    SendClientRequestTest<AppendEntriesArgs>(args, cmp);
  }
}

void SerializerTest::TestSerializeAppendEntriesReply(bool async) {
  AppendEntriesReply reply =
      AppendEntriesReply{static_cast<raft_term_t>(rand()),
                         rand(),
                         static_cast<raft_index_t>(rand()),
                         static_cast<raft_node_id_t>(rand()),
                         0, // padding
                         3,
                         {GenerateRandomChunkInfo(), GenerateRandomChunkInfo(),
                          GenerateRandomChunkInfo()}};

  auto cmp = [](const AppendEntriesReply &lhs,
                const AppendEntriesReply &rhs) -> bool {
    bool hdr_eq = lhs.reply_id == rhs.reply_id && lhs.success == rhs.success &&
                  lhs.expect_index == rhs.expect_index &&
                  lhs.term == rhs.term &&
                  lhs.chunk_info_cnt == rhs.chunk_info_cnt;

    if (lhs.chunk_infos.size() != rhs.chunk_infos.size()) {
      return false;
    }
    for (int i = 0; i < lhs.chunk_infos.size(); ++i) {
      if (!(lhs.chunk_infos[i] == rhs.chunk_infos[i])) {
        return false;
      }
    }
    return true;
  };
  if (async) {
    SendClientRequestAsyncTest<AppendEntriesReply>(reply, cmp);
  } else {
    SendClientRequestTest<AppendEntriesReply>(reply, cmp);
  }
}

void SerializerTest::TestSerializeRequestFragmentsArgs(bool async) {
  RequestFragmentsArgs args = RequestFragmentsArgs{
      static_cast<raft_term_t>(rand()), static_cast<raft_node_id_t>(rand()),
      static_cast<raft_index_t>(rand()), static_cast<raft_index_t>(rand())};

  auto cmp = [](const RequestFragmentsArgs &a,
                const RequestFragmentsArgs &b) -> bool {
    return std::memcmp(&a, &b, sizeof(RequestFragmentsArgs)) == 0;
  };
  if (async) {
    SendClientRequestAsyncTest<RequestFragmentsArgs>(args, cmp);
  } else {
    SendClientRequestTest<RequestFragmentsArgs>(args, cmp);
  }
}

void SerializerTest::TestSerializeRequestFragmentsReply(bool async) {
  RequestFragmentsReply reply =
      RequestFragmentsReply{static_cast<raft_node_id_t>(rand()),
                            static_cast<raft_term_t>(rand()),
                            static_cast<raft_index_t>(rand()),
                            true,
                            2,
                            {GenerateRandomLogEntry(true, kFragments),
                             GenerateRandomLogEntry(true, kFragments)}};
  auto cmp = [](const RequestFragmentsReply &lhs,
                const RequestFragmentsReply &rhs) -> bool {
    bool hdr_eq = lhs.reply_id == rhs.reply_id &&
                  lhs.start_index == rhs.start_index &&
                  lhs.success == rhs.success && lhs.term == rhs.term &&
                  lhs.entry_cnt == rhs.entry_cnt;

    if (lhs.fragments.size() != rhs.fragments.size()) {
      return false;
    }
    for (int i = 0; i < lhs.fragments.size(); ++i) {
      if (!(lhs.fragments[i] == rhs.fragments[i])) {
        return false;
      }
    }
    return true;
  };
  if (async) {
    SendClientRequestAsyncTest<RequestFragmentsReply>(reply, cmp);
  } else {
    SendClientRequestTest<RequestFragmentsReply>(reply, cmp);
  }
}

TEST_F(SerializerTest, DISABLED_TestSerializeSync) {
  LaunchServerThread();
  TestNoDataLogEntryTransfer(false);
  TestCompleteCommandDataLogEntryTransfer(false);
  TestFragmentDataLogEntryTransfer(false);
  TestSerializeRequestVoteArgs(false);
  TestSerializeRequestVoteReply(false);
  TestSerializeAppendEntriesArgs(false);
  TestSerializeAppendEntriesReply(false);
  TestSerializeRequestFragmentsArgs(false);
  TestSerializeRequestFragmentsReply(false);
}

TEST_F(SerializerTest, TestSerializeAsync) {
  LaunchServerThread();
  TestNoDataLogEntryTransfer(true);
  TestCompleteCommandDataLogEntryTransfer(true);
  TestFragmentDataLogEntryTransfer(true);
  TestSerializeRequestVoteArgs(true);
  TestSerializeRequestVoteReply(true);
  TestSerializeAppendEntriesArgs(true);
  TestSerializeAppendEntriesReply(true);
  TestSerializeRequestFragmentsArgs(true);
  TestSerializeRequestFragmentsReply(true);
}

} // namespace raft
  //
