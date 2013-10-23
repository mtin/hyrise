// Copyright (c) 2012 Hasso-Plattner-Institut fuer Softwaresystemtechnik GmbH. All rights reserved.
#include "io/ResourceManager.h"

#include "helper/stringhelpers.h"
#include "helper/Environment.h"
#include "storage/AbstractResource.h"

namespace hyrise {
namespace io {

ResourceManager& ResourceManager::getInstance() {
  static ResourceManager rm;
  return rm;
}

// template<typename T>
// std::unique_lock<T> lock_guard(T& mtx) {
//   return std::move(std::unique_lock<T>(mtx));
// }

size_t ResourceManager::size() const {
  pthread_rwlock_rdlock(&_rw_lock);
  auto res = _resources.size();
  pthread_rwlock_unlock(&_rw_lock);
  return res;
}

bool ResourceManager::exists(const std::string& name, const bool thread_safe) const {
  // auto lock = lock_guard(_resource_mutex) ;
  if (thread_safe) pthread_rwlock_rdlock(&_rw_lock);
  bool res = _resources.count(name) == 1;
  if (thread_safe) pthread_rwlock_unlock(&_rw_lock);
  return res;
}

void ResourceManager::assureExists(const std::string& name, const bool thread_safe, const bool release_lock_on_exception) const {
  if (thread_safe) pthread_rwlock_rdlock(&_rw_lock);
  if (!exists(name, false)) {
    if (thread_safe || release_lock_on_exception) pthread_rwlock_unlock(&_rw_lock);
    throw ResourceNotExistsException("ResourceManager: Resource \'" + name + "\' does not exist");
  }
  if (thread_safe) pthread_rwlock_unlock(&_rw_lock);
}

void ResourceManager::clear() const {
  pthread_rwlock_wrlock(&_rw_lock);
  _resources.clear();
  pthread_rwlock_unlock(&_rw_lock);
}

void ResourceManager::remove(const std::string& name) const {
  pthread_rwlock_wrlock(&_rw_lock);
  assureExists(name, false, true);
  _resources.erase(name);
  pthread_rwlock_unlock(&_rw_lock);
}

void ResourceManager::replace(const std::string& name, const  std::shared_ptr<AbstractResource>& resource) const {
  pthread_rwlock_wrlock(&_rw_lock);
  assureExists(name, false, true);
  _resources.at(name) = resource;
  pthread_rwlock_unlock(&_rw_lock);
}

void ResourceManager::add(const std::string& name, const std::shared_ptr<AbstractResource>& resource) const {
  pthread_rwlock_wrlock(&_rw_lock);
  if (exists(name, false)) {
    pthread_rwlock_unlock(&_rw_lock);
    throw ResourceAlreadyExistsException("ResourceManager: Resource '" + name + "' already exists");
  }
  _resources.insert(make_pair(name, resource));
  pthread_rwlock_unlock(&_rw_lock);
}

std::shared_ptr<AbstractResource> ResourceManager::getResource(const std::string& name) const {
  pthread_rwlock_rdlock(&_rw_lock);
  assureExists(name, false, true);
  auto res = _resources.at(name);
  pthread_rwlock_unlock(&_rw_lock);
  return res;
}

ResourceManager::resource_map ResourceManager::all() const {
  return _resources;
}

}
} // namespace hyrise::io

