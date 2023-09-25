#include "encoder.h"

#include <chrono>

#include "isa-l/erasure_code.h"
#include "log_entry.h"
#include "raft_type.h"

namespace raft {

// EncodeSlice should not modify underlying data contained by input slice
bool Encoder::EncodeSlice(const Slice &slice, int k, int m, EncodingResults *results) {
  auto encoding_size = slice.size();

  // NOTE: What if encoding_size is not divisible to k?
  auto fragment_size = (encoding_size + k - 1) / k;
  auto start_ptr = reinterpret_cast<unsigned char *>(slice.data());

  // A special case for k = 1, avoiding allocating new memories
  // i.e. The resultant slice is exactly the same as input slice
  if (k == 1) {
    for (int i = 0; i < k + m; ++i) {
      results->insert({i, slice});
    }
    return true;
  }

  // set input vector
  for (int i = 0; i < k; i++, start_ptr += fragment_size) {
    encode_input_[i] = start_ptr;
  }

  // prepare an ouput vector
  for (int i = 0; i < m; i++) {
    encode_output_[i] = new unsigned char[fragment_size];
  }

  // start encoding process
  auto t = m + k;
  auto g_tbls = new unsigned char[k * m * 32];

  gf_gen_cauchy1_matrix(encode_matrix_, t, k);
  ec_init_tables(k, m, &encode_matrix_[k * k], g_tbls);
  ec_encode_data(fragment_size, k, m, g_tbls, encode_input_, encode_output_);

  // write results: for the first k segments, their data is essentially the
  // encoding input, for the rest m segments, their data is the encoding output
  for (int i = 0; i < k + m; ++i) {
    if (i < k) {
      results->insert({i, Slice(reinterpret_cast<char *>(encode_input_[i]), fragment_size)});
    } else {
      results->insert({i, Slice(reinterpret_cast<char *>(encode_output_[i - k]), fragment_size)});
    }
  }
  return true;
}

// Given a few slices, use EC coding to generate a few paraties:
bool Encoder::EncodeSlice(const std::vector<Slice> &slices, int k, int m, std::vector<Slice> &res) {
  auto s = slices.at(0);
  auto fragment_sz = s.size();
  // Fast path:
  if (k == 1) {
    for (int i = 0; i < k + m; ++i) {
      res.push_back(s);
    }
    return true;
  }

  for (int i = 0; i < k; ++i) {
    encode_input_[i] = (unsigned char *)(slices[i].data());
    res.push_back(slices[i]);
  }

  auto alloc_data = new unsigned char[fragment_sz * m];
  for (int i = 0; i < m; ++i) {
    encode_output_[i] = alloc_data + fragment_sz * i;
    res.push_back(Slice((char *)(encode_output_[i]), fragment_sz));
  }

  // start encoding process
  auto t = m + k;
  auto g_tbls = new unsigned char[k * m * 32];

  gf_gen_cauchy1_matrix(encode_matrix_, t, k);
  ec_init_tables(k, m, &encode_matrix_[k * k], g_tbls);
  ec_encode_data(fragment_sz, k, m, g_tbls, encode_input_, encode_output_);

  return true;
}

bool Encoder::DecodeSliceHelper(const EncodingResults &fragments, int k, int m, char *data,
                                int *size) {
  assert(data != nullptr);
  assert(k != 0);

  // check if there is at least k fragments in input vector:
  if (fragments.size() < k) {
    return false;
  }

  missing_rows_.clear();
  valid_rows_.clear();

  int n = k + m;

  for (int i = 0; i < k; ++i) {
    missing_rows_.push_back(i);
  }

  // construct missing rows and valid rows vector
  for (const auto &[frag_id, slice] : fragments) {
    if (frag_id < k) {
      missing_rows_.erase(std::remove(missing_rows_.begin(), missing_rows_.end(), frag_id));
    }
    valid_rows_.push_back(frag_id);
  }

  std::sort(valid_rows_.begin(), valid_rows_.end());

  auto fragment_size = fragments.begin()->second.size();

  // allocate data for constructing the complete data
  auto complete_length = fragment_size * k;
  *size = complete_length;

  // char* complete_data = new char[complete_length + 16];
  // *results = Slice(complete_data, complete_length);

  // copy fragments data coming from encoding input to complete data
  for (const auto &[frag_id, slice] : fragments) {
    if (frag_id < k) {
      // All fragments have the same size
      assert(slice.size() == fragment_size);
      std::memcpy(data + frag_id * fragment_size, slice.data(), fragment_size);
    }
  }

  // No need to decoding
  if (missing_rows_.size() == 0) {
    return true;
  } else {  // Recover data after decoding
    gf_gen_cauchy1_matrix(encode_matrix_, n, k);

    // Construct the decode matrix
    for (int i = 0; i < k; ++i) {
      auto row = valid_rows_[i];
      for (int j = 0; j < k; ++j) {
        // Copy all valid rows in encode_matrix to errors_matrix
        errors_matrix_[i * k + j] = encode_matrix_[row * k + j];
      }
    }
    // Generate the inverse of errors matrix
    gf_invert_matrix(errors_matrix_, invert_matrix_, k);

    for (decltype(missing_rows_.size()) i = 0; i < missing_rows_.size(); ++i) {
      auto row = missing_rows_[i];
      for (int j = 0; j < k; ++j) {
        encode_matrix_[i * k + j] = invert_matrix_[row * k + j];
      }
    }

    auto g_tbls = new unsigned char[k * missing_rows_.size() * 32];
    ec_init_tables(k, missing_rows_.size(), encode_matrix_, g_tbls);

    // Start doing decoding, set input source address and output destination
    for (decltype(missing_rows_.size()) i = 0; i < missing_rows_.size(); ++i) {
      decode_output_[i] = (unsigned char *)(data + missing_rows_[i] * fragment_size);
    }

    auto iter = fragments.begin();
    for (int i = 0; i < k; ++i, ++iter) {
      decode_input_[i] = reinterpret_cast<unsigned char *>(iter->second.data());
    }

    ec_encode_data(fragment_size, k, missing_rows_.size(), g_tbls, decode_input_, decode_output_);
  }
  return true;
}

bool Encoder::DecodeSlice(const EncodingResults &fragments, int k, int m, Slice *results) {
  // For this special case, directly returns the identical entry. So that
  // DecodeSlice is a dual form of Encode Slice
  if (k == 1) {
    *results = Slice::Copy(fragments.begin()->second);
    return true;
  }

  auto fragment_size = fragments.begin()->second.size();
  auto complete_size = fragment_size * k;
  auto data = new char[complete_size + 16];
  int decode_size = 0;
  if (!DecodeSliceHelper(fragments, k, m, data, &decode_size)) {
    delete[] data;
    return false;
  }
  *results = Slice(data, decode_size);
  return true;
}

void StaticEncoder::Init(int k, int m) {
  encode_matrix_ = new unsigned char[kMaxK * (kMaxM + kMaxK)];

  auto start = std::chrono::steady_clock::now();

  auto t = m + k;
  g_tbls_ = new unsigned char[k * m * 32];
  encode_k_ = k;
  encode_m_ = m;

  gf_gen_cauchy1_matrix(encode_matrix_, t, k);
  ec_init_tables(k, m, &encode_matrix_[k * k], g_tbls_);

  auto end = std::chrono::steady_clock::now();
  auto dura = std::chrono::duration_cast<std::chrono::nanoseconds>(end - start);
  printf("StaticEncoder Init time: %lu ns\n", dura.count());
}

bool StaticEncoder::EncodeSlice(const std::vector<Slice> &input, std::vector<Slice> &output) {
  auto s = input.at(0);
  auto fragment_sz = s.size();

  for (int i = 0; i < encode_k_; ++i) {
    encode_input_[i] = (unsigned char *)(input[i].data());
    output.push_back(input[i]);
  }

  auto alloc_data = new unsigned char[fragment_sz * encode_m_];
  for (int i = 0; i < encode_m_; ++i) {
    encode_output_[i] = alloc_data + fragment_sz * i;
    output.push_back(Slice((char *)(encode_output_[i]), fragment_sz));
  }

  ec_encode_data(fragment_sz, encode_k_, encode_m_, g_tbls_, encode_input_, encode_output_);
  return true;
}

}  // namespace raft
