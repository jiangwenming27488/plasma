#include "plasma/client.h"
#include <arrow/python/pyarrow.h>
#include <exception>
#include <fmt/format.h>
#include <memory>
#include <pybind11/pybind11.h>
#include <pybind11/stl.h>
#include <pybind11_json.hpp>
#include <stdexcept>
#include <utility>

class PlasmaClient : public std::enable_shared_from_this<PlasmaClient> {
private:
  std::shared_ptr<plasma::PlasmaClient> client_;
  int notification_fd_;
  std::string store_socket_name_;
  std::string manager_socket_name_;

public:
  explicit PlasmaClient(const std::string &stock_socket_name,
                        const std::string &manager_socket_name = "");

  ~PlasmaClient() = default;

  int get_notification_fd() const;

  std::string store_socket_name() const;

  void disconnect();

  void subscribe();

  void seal(const plasma::ObjectID &object_id);

  void release(const plasma::ObjectID &object_id);

  bool contains(const plasma::ObjectID &object_id);

  std::string hash(const plasma::ObjectID &object_id);

  int64_t evict(int64_t num_bytes);
  pybind11::tuple decode_notifications(const uint8_t *buf);

  pybind11::tuple get_next_notification();

  void remove(const std::vector<plasma::ObjectID> &object_ids);

  void connect(int release_delay, int num_retries = -1);

  void set_client_options(const std::string &client_name,
                          int64_t limit_output_memory);

  int64_t store_capacity() const;

  std::string debug_string() const;

  std::unordered_map<plasma::ObjectID, nlohmann::json> list();

  std::shared_ptr<arrow::Buffer> create(const plasma::ObjectID &object_id,
                                        int64_t data_size,
                                        std::string metadata = "");

private:
  std::vector<plasma::ObjectBuffer>
  _get_object_buffers(const std::vector<plasma::ObjectID> &object_ids,
                      int64_t timeout_ms);

public:
  std::shared_ptr<arrow::Buffer>
  get_metadata_buffer(const plasma::ObjectID &object_id,
                      int64_t timeout_ms = -1);

  std::shared_ptr<arrow::Buffer> get_buffer(const plasma::ObjectID &object_id,
                                            int64_t timeout_ms = -1);
};



namespace PYBIND11_NAMESPACE {
namespace detail {
template <> struct type_caster<std::shared_ptr<arrow::Buffer>> {
public:
  PYBIND11_TYPE_CASTER(std::shared_ptr<arrow::Buffer>, _("pyarrow::Buffer"));
  bool load(handle src, bool) {
    PyObject *source = src.ptr();
    if (!arrow::py::is_buffer(source))
      return false;
    arrow::Result<std::shared_ptr<arrow::Buffer>> result =
        arrow::py::unwrap_buffer(source);
    if (!result.ok())
      return false;
    value = std::static_pointer_cast<arrow::Buffer>(result.ValueOrDie());
    return true;
  }
  static handle cast(std::shared_ptr<arrow::Buffer> src,
                     return_value_policy /* policy */, handle /* parent */) {
    return arrow::py::wrap_buffer(src);
  }
};
} // namespace detail
} // namespace PYBIND11_NAMESPACE

PYBIND11_MODULE(pyplasma, m) {
  m.attr("PLASMA_WAIT_TIMEOUT") = static_cast<unsigned long>(std::pow(2, 30));
  m.attr("kDigestSize") = plasma::kDigestSize;
  pybind11::class_<plasma::ObjectID>(m, "ObjectID")
      .def("from_binary", &plasma::ObjectID::from_binary)
      .def("binary", &plasma::ObjectID::binary)
      .def("__hash__", &plasma::ObjectID::hash)
      .def("__repr__",
           [](plasma::ObjectID &self) {
             return fmt::format("ObjectID({0})", self.hex());
           })
      .def("hex", &plasma::ObjectID::hex)
      .def_static("size", &plasma::ObjectID::size)
      .def("__eq__", &plasma::ObjectID::operator==);

  pybind11::class_<PlasmaClient, std::shared_ptr<PlasmaClient>>(m,
                                                                "PlasmaClient")
      .def("disconnect", &PlasmaClient::disconnect)
      .def("subscribe", &PlasmaClient::subscribe)
      .def("seal", &PlasmaClient::seal)
      .def("fd", &PlasmaClient::get_notification_fd)
      .def("contains", &PlasmaClient::contains)
      .def("hash", &PlasmaClient::hash)
      .def("evict", &PlasmaClient::evict)
      .def("decode_notifications", &PlasmaClient::decode_notifications)
      .def("get_next_notification", &PlasmaClient::get_next_notification)
      .def("connect", &PlasmaClient::connect)
      .def("remove", &PlasmaClient::remove)
      .def("set_client_options", &PlasmaClient::set_client_options)
      .def("store_capacity", &PlasmaClient::store_capacity)
      .def("list", &PlasmaClient::list)
      .def("store_socket_name", &PlasmaClient::store_socket_name)
      .def("create", &PlasmaClient::create)
      .def("get_meta_buffer", &PlasmaClient::get_metadata_buffer)
      .def("get_buffer", &PlasmaClient::get_buffer)
      .def("debug_string", &PlasmaClient::debug_string);
  m.def(
      "connect",
      [](const std::string &store_socket_name, int num_retries = -1) {
        std::shared_ptr<PlasmaClient> client =
            std::make_shared<PlasmaClient>(store_socket_name);
        client->connect(0, num_retries);
        return client;
      },
      "Return a new PlasmaClient that is connected a plasma store and "
      "optionally a manager.");
}

PlasmaClient::PlasmaClient(const std::string &stock_socket_name,
                           const std::string &manager_socket_name) {
  this->client_.reset(new plasma::PlasmaClient());
  this->notification_fd_ = -1;
  this->store_socket_name_ = stock_socket_name;
  this->manager_socket_name_ = manager_socket_name;
}

int PlasmaClient::get_notification_fd() const { return this->notification_fd_; }

void PlasmaClient::disconnect() {
  auto status = this->client_->Disconnect();
  if (!status.ok()) {
    throw std::runtime_error(status.message());
  }
}

void PlasmaClient::subscribe() {
  auto status = this->client_->Subscribe(&this->notification_fd_);
  if (!status.ok()) {
    throw std::runtime_error(status.message());
  }
}

void PlasmaClient::seal(const plasma::ObjectID &object_id) {
  auto status = this->client_->Seal(object_id);
  if (!status.ok()) {
    throw std::runtime_error(status.message());
  }
}

void PlasmaClient::release(const plasma::ObjectID &object_id) {
  auto status = this->client_->Release(object_id);
  if (!status.ok()) {
    throw std::runtime_error(status.message());
  }
}

bool PlasmaClient::contains(const plasma::ObjectID &object_id) {
  bool is_contained = false;
  auto status = this->client_->Contains(object_id, &is_contained);
  if (!status.ok()) {
    throw std::runtime_error(status.message());
  }
  return is_contained;
}

std::string PlasmaClient::hash(const plasma::ObjectID &object_id) {
  std::string digest{};
  digest.resize(plasma::kDigestSize);
  auto status = this->client_->Hash(object_id, (uint8_t *)digest.data());
  if (!status.ok()) {
    throw std::runtime_error(status.message());
  }
  return digest;
}

int64_t PlasmaClient::evict(int64_t num_bytes) {
  int64_t num_bytes_evicted = -1;
  auto status = this->client_->Evict(num_bytes, num_bytes_evicted);
  if (!status.ok()) {
    throw std::runtime_error(status.message());
  }
  return num_bytes_evicted;
}

pybind11::tuple PlasmaClient::decode_notifications(const uint8_t *buf) {
  std::vector<plasma::ObjectID> ids{};
  std::vector<int64_t> data_sizes{};
  std::vector<int64_t> metadata_sizes{};
  auto status = this->client_->DecodeNotifications(buf, &ids, &data_sizes,
                                                   &metadata_sizes);

  std::vector<plasma::ObjectID> object_ids{};
  object_ids.reserve(ids.size());
  for (auto const &object_id : ids) {
    object_ids.push_back(plasma::ObjectID::from_binary(object_id.binary()));
  }
  return pybind11::make_tuple(object_ids, data_sizes, metadata_sizes);
}

pybind11::tuple PlasmaClient::get_next_notification() {
  std::string id_str(plasma::ObjectID::size(), 0);
  plasma::ObjectID object_id = plasma::ObjectID::from_binary(id_str);
  int64_t data_size = 0;
  int64_t metadata_size = 0;
  auto status = this->client_->GetNotification(
      this->notification_fd_, &object_id, &data_size, &metadata_size);
  if (!status.ok()) {
    throw std::runtime_error(status.message());
  }
  return pybind11::make_tuple(object_id, data_size, metadata_size);
}

void PlasmaClient::remove(const std::vector<plasma::ObjectID> &object_ids) {
  auto status = this->client_->Delete(object_ids);
  if (!status.ok()) {
    throw std::runtime_error(status.message());
  }
}

void PlasmaClient::connect(int release_delay, int num_retries) {
  auto status = this->client_->Connect(this->store_socket_name_,
                                       this->manager_socket_name_,
                                       release_delay, num_retries);
  if (!status.ok()) {
    throw std::runtime_error(status.message());
  }
}

void PlasmaClient::set_client_options(const std::string &client_name,
                                      int64_t limit_output_memory) {
  auto status =
      this->client_->SetClientOptions(client_name, limit_output_memory);
  if (!status.ok()) {
    throw std::runtime_error(status.message());
  }
}

int64_t PlasmaClient::store_capacity() const {
  return this->client_->store_capacity();
}

std::string PlasmaClient::debug_string() const {
  return this->client_->DebugString();
}

std::unordered_map<plasma::ObjectID, nlohmann::json> PlasmaClient::list() {
  plasma::ObjectTable objects;
  auto status = this->client_->List(&objects);
  if (!status.ok()) {
    throw std::runtime_error(status.message());
  }
  std::unordered_map<plasma::ObjectID, nlohmann::json> result;
  for (auto const &object : objects) {
    std::string tmp = "sealed";
    if (object.second->state == plasma::ObjectState::PLASMA_CREATED) {
      tmp = "created";
    }
    nlohmann::json stats;
    stats["data_size"] = object.second->data_size;
    stats["metadata_size"] = object.second->metadata_size;
    stats["ref_count"] = object.second->ref_count;
    stats["create_time"] = object.second->create_time;
    stats["construct_duration"] = object.second->construct_duration;
    stats["state"] = tmp;
    stats["map_size"] = object.second->map_size;
    result[object.first] = std::move(stats);
  }
  return result;
}

std::string PlasmaClient::store_socket_name() const {
  return this->store_socket_name_;
}

std::shared_ptr<arrow::Buffer>
PlasmaClient::create(const plasma::ObjectID &object_id, int64_t data_size,
                     std::string metadata) {
  std::shared_ptr<arrow::Buffer> data;
  auto meta_len = static_cast<int64_t>(metadata.size());
  auto status = this->client_->Create(
      object_id, data_size, (uint8_t *)metadata.data(), meta_len, &data);
  if (plasma::IsPlasmaObjectExists(status)) {
    throw std::runtime_error(fmt::format(
        "current plasma object[{0}] already exists", object_id.binary()));
  } else if (plasma::IsPlasmaStoreFull(status)) {
    throw std::runtime_error("plasma store server full");
  }
  if (!status.ok()) {
    throw std::runtime_error(status.message());
  }
  return data;
}

std::vector<plasma::ObjectBuffer> PlasmaClient::_get_object_buffers(
    const std::vector<plasma::ObjectID> &object_ids, int64_t timeout_ms) {
  std::vector<plasma::ObjectBuffer> buffers{};
  auto status = this->client_->Get(object_ids, timeout_ms, &buffers);
  if (!status.ok()) {
    throw std::runtime_error(status.message());
  }
  return buffers;
}

std::shared_ptr<arrow::Buffer>
PlasmaClient::get_metadata_buffer(const plasma::ObjectID &object_id,
                                  int64_t timeout_ms) {
  std::vector<plasma::ObjectID> object_ids = {object_id};
  std::vector<plasma::ObjectBuffer> object_buffers =
      this->_get_object_buffers(object_ids, timeout_ms);
  std::shared_ptr<arrow::Buffer> buffer = nullptr;
  if (!object_buffers.empty()) {
    buffer = object_buffers.begin()->metadata;
  }
  return buffer;
}

std::shared_ptr<arrow::Buffer>
PlasmaClient::get_buffer(const plasma::ObjectID &object_id,
                         int64_t timeout_ms) {
  std::vector<plasma::ObjectID> object_ids = {object_id};
  std::vector<plasma::ObjectBuffer> buffers =
      this->_get_object_buffers(object_ids, timeout_ms);
  std::shared_ptr<arrow::Buffer> buffer = nullptr;
  if (!buffers.empty()) {
    buffer = std::move(buffers.begin()->data);
  }
  return buffer;
}
