#include <arpa/inet.h>
#include <fcntl.h>
#include <netdb.h>
#include <netinet/tcp.h>
#include <pthread.h>
#include <sys/epoll.h>
#include <sys/mman.h>
#include <sys/resource.h>
#include <sys/socket.h>
#include <sys/stat.h>
#include <unistd.h>

#include <algorithm>
#include <atomic>
#include <barrier>
#include <chrono>
#include <cmath>
#include <cstdint>
#include <cstring>
#include <deque>
#include <fstream>
#include <iomanip>
#include <iostream>
#include <latch>
#include <limits>
#include <memory>
#include <numeric>
#include <optional>
#include <random>
#include <sstream>
#include <stdexcept>
#include <string>
#include <string_view>
#include <thread>
#include <utility>
#include <vector>

#include <hdr/hdr_histogram.h>
#include <llhttp.h>

namespace {

using Clock = std::chrono::steady_clock;
using TimePoint = Clock::time_point;

constexpr std::int64_t HISTOGRAM_MIN_MAX_US = 1'000'000;
constexpr int HISTOGRAM_SIGFIGS = 3;
constexpr std::size_t MAX_ERROR_SAMPLES = 100;
constexpr int MAX_EPOLL_EVENTS = 256;

struct FailRule {
  bool fail_if_present;
  std::string marker;
};

struct Config {
  std::string workload_path;
  std::string host;
  std::string out_path;
  std::string order = "seq";
  std::uint16_t port = 0;
  std::uint32_t concurrency = 0;
  std::uint32_t threads = 1;
  std::uint32_t repetitions = 0;
  double duration_s = 0.0;
  double warmup_s = 1.0;
  // Request-count bounds, 0 = unbounded. A measured phase ends at whichever of
  // --duration/--max-requests comes first, which is what makes a dev-tier grid
  // cheap: a fast cell needs a request budget (0.25s of a 50k-qps cell is
  // 12,500 samples nobody reads) and a slow one needs a time budget.
  std::uint64_t max_requests = 0;
  // Warmup measured in requests rather than seconds. With one request per
  // connection the phase does nothing but establish sockets and fault in
  // whatever the first call touches, which is all a per-cell warmup is for
  // once the leg has already been warmed across every shape.
  std::uint64_t warmup_requests = 0;
  double timeout_s = 0.0;
  std::int64_t histogram_max_us = HISTOGRAM_MIN_MAX_US;
  pid_t server_pid = 0;
  std::optional<std::uint64_t> seed;
  std::vector<int> client_cores;
  std::vector<FailRule> fail_rules;
};

struct Address {
  sockaddr_storage storage{};
  socklen_t length = 0;
  int family = AF_UNSPEC;
};

struct Record {
  const std::uint8_t* ptr;
  std::uint32_t len;
  std::uint16_t bucket;
};

class Workload {
  int fd_ = -1;
  const std::uint8_t* base_ = nullptr;
  std::size_t size_ = 0;
  std::vector<std::string> labels_;
  std::vector<Record> records_;

 public:
  explicit Workload(const std::string& path) {
    fd_ = open(path.c_str(), O_RDONLY | O_CLOEXEC);
    if (fd_ < 0) {
      throw std::runtime_error("open workload: " + std::string(strerror(errno)));
    }
    struct stat stat_value {};
    if (fstat(fd_, &stat_value) != 0) {
      throw std::runtime_error("stat workload: " + std::string(strerror(errno)));
    }
    if (stat_value.st_size <= 0) {
      throw std::runtime_error("workload is empty");
    }
    size_ = (std::size_t)stat_value.st_size;
    void* mapped = mmap(nullptr, size_, PROT_READ, MAP_PRIVATE, fd_, 0);
    if (mapped == MAP_FAILED) {
      throw std::runtime_error("mmap workload: " + std::string(strerror(errno)));
    }
    base_ = (const std::uint8_t*)mapped;
    parse();
  }

  ~Workload() {
    if (base_ != nullptr) {
      munmap((void*)base_, size_);
    }
    if (fd_ >= 0) {
      close(fd_);
    }
  }

  Workload(const Workload&) = delete;
  Workload& operator=(const Workload&) = delete;

  const std::vector<std::string>& labels() const { return labels_; }
  const std::vector<Record>& records() const { return records_; }

 private:
  template <typename T>
  T read(const std::uint8_t*& cursor, const std::uint8_t* end) {
    if ((std::size_t)(end - cursor) < sizeof(T)) {
      throw std::runtime_error("truncated workload");
    }
    T value;
    memcpy(&value, cursor, sizeof(value));
    cursor += sizeof(value);
#if __BYTE_ORDER__ == __ORDER_BIG_ENDIAN__
    if constexpr (sizeof(T) == 2) {
      value = (T)__builtin_bswap16((std::uint16_t)value);
    } else if constexpr (sizeof(T) == 4) {
      value = (T)__builtin_bswap32((std::uint32_t)value);
    }
#endif
    return value;
  }

  void parse() {
    const std::uint8_t* cursor = base_;
    const std::uint8_t* end = base_ + size_;
    if ((std::size_t)(end - cursor) < 4 || memcmp(cursor, "SBWL", 4) != 0) {
      throw std::runtime_error("invalid workload magic");
    }
    cursor += 4;
    std::uint32_t version = read<std::uint32_t>(cursor, end);
    std::uint32_t record_count = read<std::uint32_t>(cursor, end);
    std::uint16_t bucket_count = read<std::uint16_t>(cursor, end);
    if (version != 1) {
      throw std::runtime_error("unsupported workload version " + std::to_string(version));
    }
    if (record_count == 0 || bucket_count == 0) {
      throw std::runtime_error("workload must contain records and buckets");
    }
    labels_.reserve(bucket_count);
    for (std::uint16_t bucket = 0; bucket < bucket_count; ++bucket) {
      std::uint16_t length = read<std::uint16_t>(cursor, end);
      if ((std::size_t)(end - cursor) < length) {
        throw std::runtime_error("truncated workload bucket label");
      }
      labels_.emplace_back((const char*)cursor, length);
      cursor += length;
    }
    records_.reserve(record_count);
    for (std::uint32_t index = 0; index < record_count; ++index) {
      std::uint32_t length = read<std::uint32_t>(cursor, end);
      std::uint16_t bucket = read<std::uint16_t>(cursor, end);
      if (bucket >= bucket_count) {
        throw std::runtime_error("workload record has invalid bucket id");
      }
      if ((std::size_t)(end - cursor) < length) {
        throw std::runtime_error("truncated workload request");
      }
      records_.push_back(Record{cursor, length, bucket});
      cursor += length;
    }
    if (cursor != end) {
      throw std::runtime_error("workload has trailing bytes");
    }
  }
};

class Histogram {
  hdr_histogram* histogram_ = nullptr;
  std::int64_t highest_trackable_value_ = HISTOGRAM_MIN_MAX_US;

 public:
  Histogram() = default;
  explicit Histogram(std::int64_t highest_trackable_value)
      : highest_trackable_value_(highest_trackable_value) {}

  ~Histogram() {
    if (histogram_ != nullptr) {
      hdr_close(histogram_);
    }
  }

  Histogram(const Histogram&) = delete;
  Histogram& operator=(const Histogram&) = delete;

  Histogram(Histogram&& other) noexcept
      : histogram_(other.histogram_),
        highest_trackable_value_(other.highest_trackable_value_) {
    other.histogram_ = nullptr;
  }

  Histogram& operator=(Histogram&& other) noexcept {
    if (this != &other) {
      if (histogram_ != nullptr) {
        hdr_close(histogram_);
      }
      histogram_ = other.histogram_;
      highest_trackable_value_ = other.highest_trackable_value_;
      other.histogram_ = nullptr;
    }
    return *this;
  }

  void record(std::int64_t value) {
    ensure();
    if (!hdr_record_value(histogram_, value)) {
      throw std::runtime_error("latency exceeds HDR histogram range");
    }
  }

  void add(const Histogram& other) {
    if (other.histogram_ == nullptr || other.histogram_->total_count == 0) {
      return;
    }
    if (histogram_ == nullptr) {
      highest_trackable_value_ =
          std::max(highest_trackable_value_, other.highest_trackable_value_);
    }
    ensure();
    if (hdr_add(histogram_, other.histogram_) != 0) {
      throw std::runtime_error("HDR histogram merge dropped values");
    }
  }

  void reset() {
    if (histogram_ != nullptr) {
      hdr_reset(histogram_);
    }
  }

  const hdr_histogram* get() const { return histogram_; }

  std::uint64_t count() const {
    return histogram_ == nullptr ? 0 : (std::uint64_t)histogram_->total_count;
  }

 private:
  void ensure() {
    if (histogram_ == nullptr
        && hdr_init(1, highest_trackable_value_, HISTOGRAM_SIGFIGS, &histogram_) != 0) {
      throw std::runtime_error("failed to allocate HDR histogram");
    }
  }
};

struct BucketStats {
  Histogram latency;
  std::uint64_t requests = 0;
  std::uint64_t errors = 0;
};

struct ErrorSample {
  std::uint16_t bucket;
  std::string kind;
  int status;
  std::string detail;
};

enum class PhaseKind { WARMUP, MEASURED, STOP };

struct Phase {
  PhaseKind kind = PhaseKind::STOP;
  bool duration_mode = false;
  TimePoint deadline{};
  // Requests STARTED in this phase. Counting starts rather than completions
  // means every started request also finishes, so a capped phase lands on
  // exactly max_requests instead of overshooting by the in-flight depth.
  // Atomic because threads > 1 shares one Phase across event loops, and
  // mutable because workers hold the Phase by const pointer: everything else
  // here is the coordinator's description of the phase, set between barriers
  // and read-only to workers. This one field is the workers' own tally.
  mutable std::atomic<std::uint64_t> started{0};
  std::uint64_t max_requests = 0;
  // Requests that must start before either bound is honoured. Without a floor
  // a 0.25s window over a one-second query records nothing at all and the cell
  // reports zero; with concurrency as the floor every client contributes one
  // sample no matter how slow the query is.
  std::uint64_t min_requests = 0;
};

enum class ConnectionState {
  IDLE,
  RETRY,
  CONNECTING,
  HANDSHAKE_WRITING,
  HANDSHAKE_READING,
  WRITING,
  READING
};

// How far a protocol has got with the bytes it has been handed.
enum class Frame { MORE, DONE, FAILED };

// Per-connection wire framing. The event loop owns TCP, timing, the workload
// order and the statistics; a Protocol owns only the answer to "where does one
// response end, and did it fail". HTTP/1.1 framing is handled by llhttp.
class Protocol {
 public:
  virtual ~Protocol() = default;

  // Bytes to send on a fresh socket before its first request; empty when the
  // protocol needs no handshake. Excluded from request latency: the event loop
  // starts the clock when the request itself is written.
  virtual std::string_view handshake() const { return {}; }
  virtual Frame consume_handshake(const char*, std::size_t) { return Frame::DONE; }

  virtual void begin_request() = 0;
  virtual Frame consume(const char* data, std::size_t length) = 0;
  // The peer closed the socket; decide whether what arrived was a complete
  // response.
  virtual Frame finish_eof() = 0;

  virtual int status() const = 0;
  virtual bool keep_alive() const = 0;
  // Response payload, for byte-level fail-marker scanning. Empty when the
  // protocol reports failure structurally and needs no marker scan.
  virtual const std::vector<std::uint8_t>& body() const = 0;
  virtual std::string detail() const { return {}; }
};

class HttpProtocol final : public Protocol {
  llhttp_t parser_{};
  std::vector<std::uint8_t> body_;
  int status_ = 0;
  bool keep_alive_ = false;
  bool complete_ = false;
  bool scan_body_ = false;
  std::string error_;

 public:
  explicit HttpProtocol(bool scan_body) : scan_body_(scan_body) {
    llhttp_init(&parser_, HTTP_RESPONSE, settings());
    parser_.data = this;
  }

  void begin_request() override {
    llhttp_reset(&parser_);
    parser_.data = this;
    body_.clear();
    status_ = 0;
    keep_alive_ = false;
    complete_ = false;
    error_.clear();
  }

  Frame consume(const char* data, std::size_t length) override {
    llhttp_errno_t result = llhttp_execute(&parser_, data, length);
    if (result != HPE_OK) {
      const char* reason = llhttp_get_error_reason(&parser_);
      error_ = "HTTP parse error " + std::string(llhttp_errno_name(result));
      if (reason != nullptr) {
        error_ += ": ";
        error_ += reason;
      }
      return Frame::FAILED;
    }
    return complete_ ? Frame::DONE : Frame::MORE;
  }

  Frame finish_eof() override {
    llhttp_errno_t result = llhttp_finish(&parser_);
    if (result == HPE_OK && complete_) {
      return Frame::DONE;
    }
    error_ = "connection closed before complete response";
    if (result != HPE_OK) {
      error_ += ": ";
      error_ += llhttp_errno_name(result);
    }
    return Frame::FAILED;
  }

  int status() const override { return status_; }
  bool keep_alive() const override { return keep_alive_; }
  const std::vector<std::uint8_t>& body() const override { return body_; }
  std::string detail() const override { return error_; }

 private:
  static int on_body(llhttp_t* parser, const char* data, std::size_t length) {
    HttpProtocol* self = (HttpProtocol*)parser->data;
    if (self->scan_body_) {
      const std::uint8_t* begin = (const std::uint8_t*)data;
      self->body_.insert(self->body_.end(), begin, begin + length);
    }
    return 0;
  }

  static int on_message_complete(llhttp_t* parser) {
    HttpProtocol* self = (HttpProtocol*)parser->data;
    // llhttp resets message flags when it is ready to parse a subsequent
    // response from the same input span. Capture message-local state here.
    self->status_ = llhttp_get_status_code(parser);
    self->keep_alive_ = llhttp_should_keep_alive(parser);
    self->complete_ = true;
    return 0;
  }

  static const llhttp_settings_t* settings() {
    static llhttp_settings_t value;
    static bool initialized = false;
    if (!initialized) {
      llhttp_settings_init(&value);
      value.on_body = on_body;
      value.on_message_complete = on_message_complete;
      initialized = true;
    }
    return &value;
  }
};


struct Connection {
  std::unique_ptr<Protocol> protocol;
  const Record* record = nullptr;
  std::uint64_t cursor = 0;
  std::uint32_t global_index;
  std::uint32_t registered_events = 0;
  int fd = -1;
  ConnectionState state = ConnectionState::IDLE;
  const std::uint8_t* write_ptr = nullptr;
  std::size_t write_len = 0;
  std::size_t write_offset = 0;
  TimePoint request_deadline{};
  TimePoint retry_at{};
  TimePoint request_started{};
  bool write_started = false;

  Connection(std::uint32_t index, std::unique_ptr<Protocol> proto)
      : protocol(std::move(proto)), global_index(index) {}
};

class EventLoop {
  const Config& config_;
  const Address& address_;
  const Workload& workload_;
  const std::vector<std::uint32_t>& order_;
  int epoll_fd_ = -1;
  std::deque<Connection> connections_;
  std::vector<Histogram> histograms_;
  std::vector<std::uint64_t> successes_;
  std::vector<std::uint64_t> errors_;
  std::vector<ErrorSample> samples_;
  const Phase* phase_ = nullptr;
  std::size_t active_ = 0;

 public:
  EventLoop(const Config& config, const Address& address, const Workload& workload,
            const std::vector<std::uint32_t>& order, std::uint32_t first_connection,
            std::uint32_t connection_count)
      : config_(config),
        address_(address),
        workload_(workload),
        order_(order),
        successes_(workload.labels().size()),
        errors_(workload.labels().size()) {
    epoll_fd_ = epoll_create1(EPOLL_CLOEXEC);
    if (epoll_fd_ < 0) {
      throw std::runtime_error("epoll_create1: " + std::string(strerror(errno)));
    }
    histograms_.reserve(workload.labels().size());
    for (std::size_t bucket = 0; bucket < workload.labels().size(); ++bucket) {
      histograms_.emplace_back(config.histogram_max_us);
    }
    for (std::uint32_t offset = 0; offset < connection_count; ++offset) {
      connections_.emplace_back(first_connection + offset, make_protocol());
    }
  }

  std::unique_ptr<Protocol> make_protocol() const {
    return std::make_unique<HttpProtocol>(!config_.fail_rules.empty());
  }

  ~EventLoop() {
    for (Connection& connection : connections_) {
      close_socket(connection);
    }
    if (epoll_fd_ >= 0) {
      close(epoll_fd_);
    }
  }

  EventLoop(const EventLoop&) = delete;
  EventLoop& operator=(const EventLoop&) = delete;

  const std::vector<Histogram>& histograms() const { return histograms_; }
  const std::vector<std::uint64_t>& successes() const { return successes_; }
  const std::vector<std::uint64_t>& errors() const { return errors_; }
  const std::vector<ErrorSample>& samples() const { return samples_; }

  void run(const Phase& phase) {
    phase_ = &phase;
    active_ = 0;
    if (phase.kind == PhaseKind::MEASURED) {
      std::fill(successes_.begin(), successes_.end(), 0);
      std::fill(errors_.begin(), errors_.end(), 0);
      for (Histogram& histogram : histograms_) {
        histogram.reset();
      }
    }
    for (Connection& connection : connections_) {
      if (connection.state != ConnectionState::IDLE) {
        throw std::runtime_error("connection not idle at phase boundary");
      }
      connection.cursor = connection.global_index;
      if (assign_and_start(connection)) {
        ++active_;
      }
    }
    event_loop();
    phase_ = nullptr;
  }

 private:
  bool recording() const { return phase_->kind == PhaseKind::MEASURED; }

  bool has_next(const Connection& connection, TimePoint now) const {
    std::uint64_t started = phase_->started.load(std::memory_order_relaxed);
    if (phase_->max_requests > 0 && started >= phase_->max_requests) {
      return false;
    }
    // Below the floor neither bound applies: see Phase::min_requests.
    if (started < phase_->min_requests) {
      return true;
    }
    if (phase_->duration_mode) {
      return now < phase_->deadline;
    }
    return connection.cursor < order_.size();
  }

  bool assign_and_start(Connection& connection) {
    TimePoint now = Clock::now();
    if (!has_next(connection, now)) {
      connection.state = ConnectionState::IDLE;
      connection.record = nullptr;
      return false;
    }
    phase_->started.fetch_add(1, std::memory_order_relaxed);
    std::uint64_t position = phase_->duration_mode
                                 ? connection.cursor % order_.size()
                                 : connection.cursor;
    connection.cursor += config_.concurrency;
    connection.record = &workload_.records()[order_[(std::size_t)position]];
    connection.request_deadline =
        now + std::chrono::duration_cast<Clock::duration>(
                  std::chrono::duration<double>(config_.timeout_s));
    if (connection.fd < 0) {
      open_connection(connection);
    } else {
      begin_write(connection);
    }
    return true;
  }

  // A future protocol may owe the server a handshake before its first request.
  // Handshake bytes stay outside the request latency clock.
  void start_after_connect(Connection& connection) {
    std::string_view handshake = connection.protocol->handshake();
    if (handshake.empty()) {
      begin_write(connection);
      return;
    }
    connection.write_ptr = (const std::uint8_t*)handshake.data();
    connection.write_len = handshake.size();
    connection.write_offset = 0;
    connection.state = ConnectionState::HANDSHAKE_WRITING;
    handle_write(connection);
  }

  void open_connection(Connection& connection) {
    connection.fd =
        socket(address_.family, SOCK_STREAM | SOCK_NONBLOCK | SOCK_CLOEXEC, 0);
    if (connection.fd < 0) {
      connect_failed(connection, "socket: " + std::string(strerror(errno)));
      return;
    }
    int enabled = 1;
    if (setsockopt(connection.fd, IPPROTO_TCP, TCP_NODELAY, &enabled, sizeof(enabled))
        != 0) {
      std::string detail = "TCP_NODELAY: " + std::string(strerror(errno));
      close_socket(connection);
      connect_failed(connection, detail);
      return;
    }
    epoll_event event{};
    event.events = EPOLLOUT | EPOLLERR | EPOLLHUP | EPOLLRDHUP;
    event.data.ptr = &connection;
    if (epoll_ctl(epoll_fd_, EPOLL_CTL_ADD, connection.fd, &event) != 0) {
      std::string detail = "epoll add: " + std::string(strerror(errno));
      close(connection.fd);
      connection.fd = -1;
      connect_failed(connection, detail);
      return;
    }
    connection.registered_events = EPOLLOUT;
    int result =
        connect(connection.fd, (const sockaddr*)&address_.storage, address_.length);
    if (result == 0) {
      start_after_connect(connection);
    } else if (errno == EINPROGRESS) {
      connection.state = ConnectionState::CONNECTING;
    } else {
      std::string detail = "connect: " + std::string(strerror(errno));
      close_socket(connection);
      connect_failed(connection, detail);
    }
  }

  void connect_failed(Connection& connection, const std::string& detail) {
    record_error(connection, "transport", -1, detail);
    connection.record = nullptr;
    connection.state = ConnectionState::RETRY;
    connection.retry_at = Clock::now() + std::chrono::milliseconds(1);
  }

  void begin_write(Connection& connection) {
    connection.protocol->begin_request();
    connection.write_ptr = connection.record->ptr;
    connection.write_len = connection.record->len;
    connection.write_offset = 0;
    connection.write_started = false;
    connection.state = ConnectionState::WRITING;
    // A connected socket is normally writable. Sending here avoids an
    // epoll round trip between every completed response and its successor.
    handle_write(connection);
  }

  void modify_events(Connection& connection, std::uint32_t events) {
    if (connection.registered_events == events) {
      return;
    }
    epoll_event event{};
    event.events = events | EPOLLERR | EPOLLHUP | EPOLLRDHUP;
    event.data.ptr = &connection;
    if (epoll_ctl(epoll_fd_, EPOLL_CTL_MOD, connection.fd, &event) != 0) {
      throw std::runtime_error("epoll mod: " + std::string(strerror(errno)));
    }
    connection.registered_events = events;
  }

  void close_socket(Connection& connection) {
    if (connection.fd >= 0) {
      epoll_ctl(epoll_fd_, EPOLL_CTL_DEL, connection.fd, nullptr);
      close(connection.fd);
      connection.fd = -1;
      connection.registered_events = 0;
    }
  }

  void finish_current(Connection& connection) {
    connection.record = nullptr;
    connection.state = ConnectionState::IDLE;
    if (!assign_and_start(connection)) {
      --active_;
    }
  }

  void fail_current(Connection& connection, const std::string& kind,
                    const std::string& detail) {
    record_error(connection, kind, -1, detail);
    close_socket(connection);
    finish_current(connection);
  }

  void record_error(Connection& connection, const std::string& kind, int status,
                    const std::string& detail) {
    if (!recording() || connection.record == nullptr) {
      return;
    }
    ++errors_[connection.record->bucket];
    if (samples_.size() < MAX_ERROR_SAMPLES) {
      samples_.push_back(ErrorSample{connection.record->bucket, kind, status, detail});
    }
  }

  void handle_connect(Connection& connection) {
    int socket_error = 0;
    socklen_t length = sizeof(socket_error);
    if (getsockopt(connection.fd, SOL_SOCKET, SO_ERROR, &socket_error, &length) != 0) {
      fail_current(connection, "transport",
                   "getsockopt(SO_ERROR): " + std::string(strerror(errno)));
      return;
    }
    if (socket_error != 0) {
      fail_current(connection, "transport",
                   "connect: " + std::string(strerror(socket_error)));
      return;
    }
    start_after_connect(connection);
  }

  void handle_write(Connection& connection) {
    bool handshaking = connection.state == ConnectionState::HANDSHAKE_WRITING;
    if (!handshaking && !connection.write_started) {
      // Closed-loop latency starts immediately before the first write attempt.
      connection.request_started = Clock::now();
      connection.write_started = true;
    }
    while (connection.write_offset < connection.write_len) {
      const std::uint8_t* data = connection.write_ptr + connection.write_offset;
      std::size_t remaining = connection.write_len - connection.write_offset;
      ssize_t written =
          send(connection.fd, data, remaining, MSG_NOSIGNAL);
      if (written > 0) {
        connection.write_offset += (std::size_t)written;
        continue;
      }
      if (written < 0 && errno == EINTR) {
        continue;
      }
      if (written < 0 && (errno == EAGAIN || errno == EWOULDBLOCK)) {
        modify_events(connection, EPOLLOUT);
        return;
      }
      std::string detail =
          written == 0 ? "write returned zero" : "write: " + std::string(strerror(errno));
      fail_current(connection, "transport", detail);
      return;
    }
    connection.state = handshaking ? ConnectionState::HANDSHAKE_READING
                                   : ConnectionState::READING;
    modify_events(connection, EPOLLIN);
  }

  bool contains_marker(const Connection& connection, const std::string& marker) const {
    if (marker.empty()) {
      return true;
    }
    const std::vector<std::uint8_t>& body = connection.protocol->body();
    if (body.size() < marker.size()) {
      return false;
    }
    return memmem(body.data(), body.size(), marker.data(), marker.size()) != nullptr;
  }

  void complete_response(Connection& connection, bool peer_closed) {
    int status = connection.protocol->status();
    bool keep_alive = !peer_closed && connection.protocol->keep_alive();
    bool failed = false;
    if (status < 200 || status >= 300) {
      std::string detail = connection.protocol->detail();
      record_error(connection, "protocol", status,
                   detail.empty() ? "status " + std::to_string(status) : detail);
      failed = true;
    } else {
      for (const FailRule& rule : config_.fail_rules) {
        bool present = contains_marker(connection, rule.marker);
        if ((rule.fail_if_present && present) || (!rule.fail_if_present && !present)) {
          std::string relation = rule.fail_if_present ? "contains" : "lacks";
          record_error(connection, "payload", status,
                       "body " + relation + " fail marker " + rule.marker);
          failed = true;
          break;
        }
      }
    }
    if (!failed && recording()) {
      std::int64_t latency_us =
          std::chrono::duration_cast<std::chrono::microseconds>(
              Clock::now() - connection.request_started)
              .count();
      latency_us = std::max<std::int64_t>(1, latency_us);
      histograms_[connection.record->bucket].record(latency_us);
      ++successes_[connection.record->bucket];
    }
    if (!keep_alive) {
      close_socket(connection);
    }
    finish_current(connection);
  }

  // Returns true when this connection is done reading for now: the response
  // completed (and the next request was dispatched) or the connection failed
  // and was reassigned. The caller must then return to epoll instead of
  // recv()ing again. A speculative recv on a keep-alive socket right after
  // dispatching the next request is a guaranteed EAGAIN on localhost - the
  // server has not even seen the request yet - i.e. one wasted syscall per
  // request in a syscall-bound loop. Returns false only while a response is
  // still arriving in fragments, where another recv is genuinely warranted.
  bool parse_bytes(Connection& connection, const char* data, std::size_t length) {
    bool handshaking = connection.state == ConnectionState::HANDSHAKE_READING;
    Frame frame = handshaking ? connection.protocol->consume_handshake(data, length)
                              : connection.protocol->consume(data, length);
    if (frame == Frame::FAILED) {
      fail_current(connection, "transport", connection.protocol->detail());
      return true;
    }
    if (frame == Frame::MORE) {
      return false;
    }
    if (handshaking) {
      // Handshake done; the request itself has not been written yet.
      begin_write(connection);
      return true;
    }
    complete_response(connection, false);
    return true;
  }

  void handle_read(Connection& connection) {
    char buffer[64 * 1024];
    while (connection.state == ConnectionState::READING
           || connection.state == ConnectionState::HANDSHAKE_READING) {
      ssize_t count = recv(connection.fd, buffer, sizeof(buffer), 0);
      if (count > 0) {
        if (parse_bytes(connection, buffer, (std::size_t)count)) {
          return;
        }
        continue;
      }
      if (count == 0) {
        if (connection.state == ConnectionState::HANDSHAKE_READING) {
          fail_current(connection, "transport", "connection closed during handshake");
          return;
        }
        if (connection.protocol->finish_eof() == Frame::DONE) {
          close_socket(connection);
          complete_response(connection, true);
        } else {
          fail_current(connection, "transport", connection.protocol->detail());
        }
        return;
      }
      if (errno == EINTR) {
        continue;
      }
      if (errno == EAGAIN || errno == EWOULDBLOCK) {
        return;
      }
      fail_current(connection, "transport", "read: " + std::string(strerror(errno)));
      return;
    }
  }

  void handle_event(Connection& connection, std::uint32_t events, TimePoint now) {
    if (connection.state == ConnectionState::IDLE
        || connection.state == ConnectionState::RETRY) {
      if ((events & (EPOLLRDHUP | EPOLLHUP | EPOLLERR)) != 0) {
        close_socket(connection);
      }
      return;
    }
    if (connection.record != nullptr && now >= connection.request_deadline) {
      fail_current(connection, "timeout", "per-request timeout");
      return;
    }
    if (connection.state == ConnectionState::CONNECTING) {
      handle_connect(connection);
      return;
    }
    const Record* record = connection.record;
    if ((connection.state == ConnectionState::WRITING
         || connection.state == ConnectionState::HANDSHAKE_WRITING)
        && (events & EPOLLOUT) != 0) {
      handle_write(connection);
      if (connection.record != record) {
        return;
      }
    }
    if ((connection.state == ConnectionState::READING
         || connection.state == ConnectionState::HANDSHAKE_READING)
        && (events & (EPOLLIN | EPOLLHUP | EPOLLRDHUP)) != 0) {
      handle_read(connection);
      return;
    }
    if (connection.state != ConnectionState::IDLE
        && connection.state != ConnectionState::RETRY
        && (events & EPOLLERR) != 0) {
      fail_current(connection, "transport", "socket error");
    } else if ((connection.state == ConnectionState::WRITING
                || connection.state == ConnectionState::HANDSHAKE_WRITING)
               && (events & (EPOLLHUP | EPOLLRDHUP)) != 0) {
      fail_current(connection, "transport", "connection closed while writing");
    }
  }

  void service_timers(TimePoint now) {
    for (Connection& connection : connections_) {
      if (connection.state == ConnectionState::RETRY && now >= connection.retry_at) {
        connection.state = ConnectionState::IDLE;
        if (!assign_and_start(connection)) {
          --active_;
        }
      } else if (connection.record != nullptr
                 && connection.state != ConnectionState::IDLE
                 && connection.state != ConnectionState::RETRY
                 && now >= connection.request_deadline) {
        fail_current(connection, "timeout", "per-request timeout");
      }
    }
  }

  int poll_timeout_ms(TimePoint now) const {
    TimePoint wake = now + std::chrono::seconds(1);
    if (phase_->duration_mode && phase_->deadline > now) {
      wake = std::min(wake, phase_->deadline);
    }
    for (const Connection& connection : connections_) {
      if (connection.state == ConnectionState::RETRY) {
        wake = std::min(wake, connection.retry_at);
      } else if (connection.record != nullptr
                 && connection.state != ConnectionState::IDLE) {
        wake = std::min(wake, connection.request_deadline);
      }
    }
    if (wake <= now) {
      return 0;
    }
    auto micros =
        std::chrono::duration_cast<std::chrono::microseconds>(wake - now).count();
    return (int)std::min<std::int64_t>(1000, (micros + 999) / 1000);
  }

  void event_loop() {
    epoll_event events[MAX_EPOLL_EVENTS];
    while (active_ != 0) {
      TimePoint before_poll = Clock::now();
      int count =
          epoll_wait(epoll_fd_, events, MAX_EPOLL_EVENTS, poll_timeout_ms(before_poll));
      if (count < 0) {
        if (errno == EINTR) {
          continue;
        }
        throw std::runtime_error("epoll_wait: " + std::string(strerror(errno)));
      }
      TimePoint now = Clock::now();
      for (int index = 0; index < count; ++index) {
        Connection* connection = (Connection*)events[index].data.ptr;
        handle_event(*connection, events[index].events, now);
      }
      service_timers(Clock::now());
    }
  }
};

struct RepetitionResult {
  int repetition;
  double elapsed_s;
  double server_cpu_s;
  double client_cpu_s;
  std::vector<BucketStats> buckets;
};

std::string require_value(int argc, char** argv, int& index, const std::string& option) {
  if (++index >= argc) {
    throw std::runtime_error(option + " requires a value");
  }
  return argv[index];
}

std::uint64_t parse_uint(const std::string& text, const std::string& option) {
  std::size_t used = 0;
  unsigned long long value;
  try {
    value = std::stoull(text, &used);
  } catch (const std::exception&) {
    throw std::runtime_error(option + " expects an unsigned integer");
  }
  if (used != text.size()) {
    throw std::runtime_error(option + " expects an unsigned integer");
  }
  return (std::uint64_t)value;
}

double parse_double(const std::string& text, const std::string& option) {
  std::size_t used = 0;
  double value;
  try {
    value = std::stod(text, &used);
  } catch (const std::exception&) {
    throw std::runtime_error(option + " expects a number");
  }
  if (used != text.size() || !std::isfinite(value)) {
    throw std::runtime_error(option + " expects a finite number");
  }
  return value;
}

std::vector<int> parse_cores(const std::string& text) {
  std::vector<int> cores;
  std::size_t start = 0;
  while (start < text.size()) {
    std::size_t comma = text.find(',', start);
    std::string part = text.substr(start, comma - start);
    std::size_t dash = part.find('-');
    int first = (int)parse_uint(part.substr(0, dash), "--client-cores");
    int last =
        dash == std::string::npos
            ? first
            : (int)parse_uint(part.substr(dash + 1), "--client-cores");
    if (first < 0 || last < first || last >= CPU_SETSIZE) {
      throw std::runtime_error("--client-cores contains an invalid range");
    }
    for (int core = first; core <= last; ++core) {
      cores.push_back(core);
    }
    if (comma == std::string::npos) {
      break;
    }
    start = comma + 1;
  }
  return cores;
}

Config parse_args(int argc, char** argv) {
  Config config;
  for (int index = 1; index < argc; ++index) {
    std::string option = argv[index];
    if (option == "--workload") {
      config.workload_path = require_value(argc, argv, index, option);
    } else if (option == "--host") {
      config.host = require_value(argc, argv, index, option);
    } else if (option == "--port") {
      std::uint64_t value =
          parse_uint(require_value(argc, argv, index, option), option);
      if (value == 0 || value > 65535) {
        throw std::runtime_error("--port must be in 1..65535");
      }
      config.port = (std::uint16_t)value;
    } else if (option == "--concurrency") {
      config.concurrency = (std::uint32_t)parse_uint(
          require_value(argc, argv, index, option), option);
    } else if (option == "--threads") {
      config.threads = (std::uint32_t)parse_uint(
          require_value(argc, argv, index, option), option);
    } else if (option == "--client-cores") {
      config.client_cores =
          parse_cores(require_value(argc, argv, index, option));
    } else if (option == "--duration") {
      config.duration_s = parse_double(require_value(argc, argv, index, option), option);
    } else if (option == "--repetitions") {
      config.repetitions = (std::uint32_t)parse_uint(
          require_value(argc, argv, index, option), option);
    } else if (option == "--order") {
      config.order = require_value(argc, argv, index, option);
    } else if (option == "--seed") {
      config.seed = parse_uint(require_value(argc, argv, index, option), option);
    } else if (option == "--warmup-seconds") {
      config.warmup_s = parse_double(require_value(argc, argv, index, option), option);
    } else if (option == "--max-requests") {
      config.max_requests = parse_uint(require_value(argc, argv, index, option), option);
    } else if (option == "--warmup-requests") {
      config.warmup_requests =
          parse_uint(require_value(argc, argv, index, option), option);
    } else if (option == "--timeout") {
      config.timeout_s = parse_double(require_value(argc, argv, index, option), option);
    } else if (option == "--server-pid") {
      config.server_pid = (pid_t)parse_uint(
          require_value(argc, argv, index, option), option);
    } else if (option == "--fail") {
      std::string rule = require_value(argc, argv, index, option);
      std::size_t colon = rule.find(':');
      if (colon == std::string::npos || colon + 1 == rule.size()) {
        throw std::runtime_error("--fail expects present:SUBSTR or absent:SUBSTR");
      }
      std::string kind = rule.substr(0, colon);
      if (kind != "present" && kind != "absent") {
        throw std::runtime_error("--fail expects present:SUBSTR or absent:SUBSTR");
      }
      config.fail_rules.push_back(FailRule{kind == "present", rule.substr(colon + 1)});
    } else if (option == "--out") {
      config.out_path = require_value(argc, argv, index, option);
    } else {
      throw std::runtime_error("unknown option " + option);
    }
  }
  if (config.workload_path.empty() || config.host.empty() || config.port == 0
      || config.concurrency == 0 || config.repetitions == 0 || config.timeout_s <= 0.0
      || config.server_pid <= 0 || config.out_path.empty()) {
    throw std::runtime_error(
        "required: --workload --host --port --concurrency --repetitions "
        "--timeout --server-pid --out");
  }
  if (config.threads == 0 || config.threads > config.concurrency) {
    throw std::runtime_error("--threads must be in 1..concurrency");
  }
  if (config.client_cores.size() < config.threads) {
    throw std::runtime_error("--client-cores must provide at least one core per thread");
  }
  if (config.duration_s < 0.0 || config.warmup_s < 0.0) {
    throw std::runtime_error("--duration and --warmup-seconds must be non-negative");
  }
  if (config.order != "seq" && config.order != "shuffle") {
    throw std::runtime_error("--order must be seq or shuffle");
  }
  if (config.order == "shuffle" && !config.seed.has_value()) {
    throw std::runtime_error("--seed is required for shuffle order");
  }
  long double histogram_max_us =
      std::ceil((long double)config.timeout_s * 1'000'000.0L);
  if (histogram_max_us > (long double)std::numeric_limits<std::int64_t>::max()) {
    throw std::runtime_error("--timeout is too large for the HDR histogram");
  }
  config.histogram_max_us = std::max(
      HISTOGRAM_MIN_MAX_US, (std::int64_t)histogram_max_us);
  return config;
}

Address resolve_address(const Config& config) {
  addrinfo hints{};
  hints.ai_family = AF_UNSPEC;
  hints.ai_socktype = SOCK_STREAM;
  hints.ai_protocol = IPPROTO_TCP;
  addrinfo* results = nullptr;
  std::string service = std::to_string(config.port);
  int status = getaddrinfo(config.host.c_str(), service.c_str(), &hints, &results);
  if (status != 0) {
    throw std::runtime_error("getaddrinfo: " + std::string(gai_strerror(status)));
  }
  Address address;
  for (addrinfo* item = results; item != nullptr; item = item->ai_next) {
    if (item->ai_addrlen <= sizeof(address.storage)) {
      memcpy(&address.storage, item->ai_addr, item->ai_addrlen);
      address.length = (socklen_t)item->ai_addrlen;
      address.family = item->ai_family;
      break;
    }
  }
  freeaddrinfo(results);
  if (address.length == 0) {
    throw std::runtime_error("host resolved to no usable TCP address");
  }
  return address;
}

double server_cpu_seconds(pid_t pid) {
  std::ifstream input("/proc/" + std::to_string(pid) + "/stat");
  std::string text;
  if (!input || !std::getline(input, text)) {
    throw std::runtime_error("cannot read /proc/" + std::to_string(pid) + "/stat");
  }
  std::size_t close_paren = text.rfind(')');
  if (close_paren == std::string::npos) {
    throw std::runtime_error("malformed server /proc stat");
  }
  std::istringstream fields(text.substr(close_paren + 1));
  std::vector<std::string> values;
  std::string value;
  while (fields >> value) {
    values.push_back(value);
  }
  if (values.size() <= 12) {
    throw std::runtime_error("truncated server /proc stat");
  }
  std::uint64_t utime = parse_uint(values[11], "server utime");
  std::uint64_t stime = parse_uint(values[12], "server stime");
  long ticks = sysconf(_SC_CLK_TCK);
  if (ticks <= 0) {
    throw std::runtime_error("sysconf(_SC_CLK_TCK) failed");
  }
  return (double)(utime + stime) / (double)ticks;
}

// Our own CPU across all event-loop threads. A cell whose replay threads run
// near saturation is client-bound and its throughput is a client artifact.
double self_cpu_seconds() {
  rusage usage;
  if (getrusage(RUSAGE_SELF, &usage) != 0) {
    throw std::runtime_error("getrusage: " + std::string(strerror(errno)));
  }
  auto seconds = [](const timeval& time) {
    return (double)time.tv_sec + (double)time.tv_usec / 1e6;
  };
  return seconds(usage.ru_utime) + seconds(usage.ru_stime);
}

void pin_thread(int core) {
  cpu_set_t cpus;
  CPU_ZERO(&cpus);
  CPU_SET(core, &cpus);
  int status = pthread_setaffinity_np(pthread_self(), sizeof(cpus), &cpus);
  if (status != 0) {
    throw std::runtime_error("pin thread to CPU " + std::to_string(core) + ": "
                             + std::string(strerror(status)));
  }
}

std::vector<BucketStats> merge_stats(const std::vector<std::unique_ptr<EventLoop>>& loops,
                                     std::size_t bucket_count) {
  std::vector<BucketStats> result(bucket_count);
  for (const std::unique_ptr<EventLoop>& loop : loops) {
    for (std::size_t bucket = 0; bucket < bucket_count; ++bucket) {
      result[bucket].requests += loop->successes()[bucket];
      result[bucket].errors += loop->errors()[bucket];
      result[bucket].latency.add(loop->histograms()[bucket]);
    }
  }
  return result;
}

BucketStats merge_buckets(const std::vector<BucketStats>& buckets) {
  BucketStats result;
  for (const BucketStats& bucket : buckets) {
    result.requests += bucket.requests;
    result.errors += bucket.errors;
    result.latency.add(bucket.latency);
  }
  return result;
}

std::string json_escape(std::string_view value) {
  std::ostringstream output;
  output << '"';
  for (unsigned char byte : value) {
    switch (byte) {
      case '"':
        output << "\\\"";
        break;
      case '\\':
        output << "\\\\";
        break;
      case '\b':
        output << "\\b";
        break;
      case '\f':
        output << "\\f";
        break;
      case '\n':
        output << "\\n";
        break;
      case '\r':
        output << "\\r";
        break;
      case '\t':
        output << "\\t";
        break;
      default:
        if (byte < 0x20) {
          output << "\\u" << std::hex << std::setw(4) << std::setfill('0')
                 << (unsigned int)byte << std::dec << std::setfill(' ');
        } else {
          output << (char)byte;
        }
    }
  }
  output << '"';
  return output.str();
}

void write_latency(std::ostream& output, const Histogram& histogram) {
  const hdr_histogram* value = histogram.get();
  if (value == nullptr || value->total_count == 0) {
    output << "{\"p50\":null,\"p90\":null,\"p99\":null,\"p999\":null,"
              "\"min\":null,\"max\":null,\"mean\":null}";
    return;
  }
  output << "{\"p50\":" << hdr_value_at_percentile(value, 50.0)
         << ",\"p90\":" << hdr_value_at_percentile(value, 90.0)
         << ",\"p99\":" << hdr_value_at_percentile(value, 99.0)
         << ",\"p999\":" << hdr_value_at_percentile(value, 99.9)
         << ",\"min\":" << hdr_min(value) << ",\"max\":" << hdr_max(value)
         << ",\"mean\":" << std::setprecision(10) << hdr_mean(value) << '}';
}

void write_bucket(std::ostream& output, const BucketStats& bucket) {
  output << "{\"requests\":" << bucket.requests << ",\"errors\":" << bucket.errors
         << ",\"latency_us\":";
  write_latency(output, bucket.latency);
  output << '}';
}

void write_per_bucket(std::ostream& output, const std::vector<std::string>& labels,
                      const std::vector<BucketStats>& buckets) {
  output << '{';
  for (std::size_t bucket = 0; bucket < buckets.size(); ++bucket) {
    if (bucket != 0) {
      output << ',';
    }
    output << json_escape(labels[bucket]) << ':';
    write_bucket(output, buckets[bucket]);
  }
  output << '}';
}

std::string result_json(const Config& config, const Workload& workload,
                        const std::vector<RepetitionResult>& repetitions,
                        const std::vector<BucketStats>& aggregate,
                        const std::vector<ErrorSample>& errors) {
  std::ostringstream output;
  output << std::setprecision(10);
  output << "{\"schema\":\"sbdriver-1\",\"config\":{\"host\":"
         << json_escape(config.host) << ",\"port\":" << config.port
         << ",\"concurrency\":" << config.concurrency << ",\"threads\":"
         << config.threads << ",\"duration_s\":" << config.duration_s
         << ",\"max_requests\":" << config.max_requests
         << ",\"warmup_requests\":" << config.warmup_requests
         << ",\"repetitions\":" << config.repetitions << ",\"order\":"
         << json_escape(config.order) << ",\"seed\":";
  if (config.seed.has_value()) {
    output << *config.seed;
  } else {
    output << "null";
  }
  output << ",\"timeout_s\":" << config.timeout_s << "},\"workload\":{\"record_count\":"
         << workload.records().size() << ",\"buckets\":{";
  for (std::size_t bucket = 0; bucket < workload.labels().size(); ++bucket) {
    if (bucket != 0) {
      output << ',';
    }
    output << json_escape(std::to_string(bucket)) << ':'
           << json_escape(workload.labels()[bucket]);
  }
  output << "}},\"repetitions\":[";
  for (std::size_t index = 0; index < repetitions.size(); ++index) {
    if (index != 0) {
      output << ',';
    }
    const RepetitionResult& repetition = repetitions[index];
    BucketStats overall = merge_buckets(repetition.buckets);
    output << "{\"repetition\":" << repetition.repetition
           << ",\"elapsed_s\":" << repetition.elapsed_s << ",\"requests\":"
           << overall.requests << ",\"errors\":" << overall.errors
           << ",\"server_cpu_s\":" << repetition.server_cpu_s
           << ",\"client_cpu_s\":" << repetition.client_cpu_s
           << ",\"per_bucket\":";
    write_per_bucket(output, workload.labels(), repetition.buckets);
    output << ",\"overall\":";
    write_bucket(output, overall);
    output << '}';
  }
  BucketStats aggregate_overall = merge_buckets(aggregate);
  output << "],\"aggregate\":{\"per_bucket\":";
  write_per_bucket(output, workload.labels(), aggregate);
  output << ",\"overall\":";
  write_bucket(output, aggregate_overall);
  output << "},\"errors\":[";
  for (std::size_t index = 0; index < errors.size(); ++index) {
    if (index != 0) {
      output << ',';
    }
    const ErrorSample& error = errors[index];
    output << "{\"bucket\":" << json_escape(workload.labels()[error.bucket])
           << ",\"kind\":" << json_escape(error.kind);
    if (error.status >= 0) {
      output << ",\"status\":" << error.status;
    }
    output << ",\"detail\":" << json_escape(error.detail) << '}';
  }
  output << "]}\n";
  return output.str();
}

int run(const Config& config) {
  Workload workload(config.workload_path);
  Address address = resolve_address(config);
  std::vector<std::uint32_t> order(workload.records().size());
  std::iota(order.begin(), order.end(), 0);
  if (config.order == "shuffle") {
    std::mt19937_64 random(*config.seed);
    std::shuffle(order.begin(), order.end(), random);
  }

  std::vector<std::unique_ptr<EventLoop>> loops;
  loops.reserve(config.threads);
  std::uint32_t first_connection = 0;
  for (std::uint32_t thread_index = 0; thread_index < config.threads;
       ++thread_index) {
    std::uint32_t count =
        config.concurrency / config.threads
        + (thread_index < config.concurrency % config.threads ? 1 : 0);
    loops.push_back(std::make_unique<EventLoop>(
        config, address, workload, order, first_connection, count));
    first_connection += count;
  }

  Phase phase;
  std::barrier phase_barrier((std::ptrdiff_t)config.threads + 1);
  std::latch ready((std::ptrdiff_t)config.threads);
  std::vector<std::string> worker_errors(config.threads);
  std::vector<std::thread> threads;
  threads.reserve(config.threads);
  for (std::uint32_t thread_index = 0; thread_index < config.threads;
       ++thread_index) {
    threads.emplace_back([&, thread_index]() {
      try {
        pin_thread(config.client_cores[thread_index]);
      } catch (const std::exception& error) {
        worker_errors[thread_index] = error.what();
      }
      ready.count_down();
      while (true) {
        phase_barrier.arrive_and_wait();
        if (phase.kind == PhaseKind::STOP) {
          break;
        }
        if (worker_errors[thread_index].empty()) {
          try {
            loops[thread_index]->run(phase);
          } catch (const std::exception& error) {
            worker_errors[thread_index] = error.what();
          }
        }
        phase_barrier.arrive_and_wait();
      }
    });
  }
  ready.wait();

  bool stopped = false;
  auto stop_workers = [&]() {
    if (!stopped) {
      phase.kind = PhaseKind::STOP;
      phase_barrier.arrive_and_wait();
      for (std::thread& thread : threads) {
        thread.join();
      }
      stopped = true;
    }
  };
  auto check_workers = [&]() {
    for (const std::string& error : worker_errors) {
      if (!error.empty()) {
        throw std::runtime_error("replay worker: " + error);
      }
    }
  };

  std::vector<RepetitionResult> repetitions;
  std::vector<BucketStats> aggregate(workload.labels().size());
  try {
    check_workers();
    if (config.warmup_s > 0.0 || config.warmup_requests > 0) {
      phase.kind = PhaseKind::WARMUP;
      phase.duration_mode = true;
      // A request-bounded warmup still needs a deadline to bound the
      // pathological case (an engine so slow it never reaches the count), so
      // it gets a generous one rather than none.
      double warmup_window = config.warmup_s > 0.0 ? config.warmup_s : 3600.0;
      phase.deadline =
          Clock::now() + std::chrono::duration_cast<Clock::duration>(
                             std::chrono::duration<double>(warmup_window));
      phase.max_requests = config.warmup_requests;
      phase.min_requests = 0;
      phase.started.store(0, std::memory_order_relaxed);
      phase_barrier.arrive_and_wait();
      phase_barrier.arrive_and_wait();
      check_workers();
    }
    for (std::uint32_t repetition = 0; repetition < config.repetitions;
         ++repetition) {
      double cpu_before = server_cpu_seconds(config.server_pid);
      double client_cpu_before = self_cpu_seconds();
      phase.kind = PhaseKind::MEASURED;
      phase.duration_mode = config.duration_s > 0.0;
      phase.deadline =
          Clock::now() + std::chrono::duration_cast<Clock::duration>(
                             std::chrono::duration<double>(config.duration_s));
      phase.max_requests = config.max_requests;
      // Every client contributes at least one sample even under a deadline
      // shorter than one request.
      phase.min_requests = config.duration_s > 0.0 ? config.concurrency : 0;
      phase.started.store(0, std::memory_order_relaxed);
      TimePoint started = Clock::now();
      phase_barrier.arrive_and_wait();
      phase_barrier.arrive_and_wait();
      double elapsed_s = std::chrono::duration<double>(Clock::now() - started).count();
      double cpu_s = server_cpu_seconds(config.server_pid) - cpu_before;
      double client_cpu_s = self_cpu_seconds() - client_cpu_before;
      check_workers();
      std::vector<BucketStats> buckets =
          merge_stats(loops, workload.labels().size());
      for (std::size_t bucket = 0; bucket < buckets.size(); ++bucket) {
        aggregate[bucket].requests += buckets[bucket].requests;
        aggregate[bucket].errors += buckets[bucket].errors;
        aggregate[bucket].latency.add(buckets[bucket].latency);
      }
      repetitions.push_back(RepetitionResult{
          (int)repetition + 1, elapsed_s, cpu_s, client_cpu_s,
          std::move(buckets)});
    }
    stop_workers();
  } catch (...) {
    stop_workers();
    throw;
  }

  std::vector<ErrorSample> errors;
  for (const std::unique_ptr<EventLoop>& loop : loops) {
    for (const ErrorSample& error : loop->samples()) {
      if (errors.size() == MAX_ERROR_SAMPLES) {
        break;
      }
      errors.push_back(error);
    }
  }
  std::string json = result_json(config, workload, repetitions, aggregate, errors);
  std::ofstream destination(config.out_path);
  if (!destination) {
    throw std::runtime_error("open output " + config.out_path + " failed");
  }
  destination << json;
  if (!destination) {
    throw std::runtime_error("write output " + config.out_path + " failed");
  }
  std::cout << json;

  std::uint64_t error_count = 0;
  for (const BucketStats& bucket : aggregate) {
    error_count += bucket.errors;
  }
  return error_count == 0 ? 0 : 1;
}

}  // namespace

int main(int argc, char** argv) {
  try {
    return run(parse_args(argc, argv));
  } catch (const std::exception& error) {
    std::cerr << "bench_replay: " << error.what() << '\n';
    return 2;
  }
}
