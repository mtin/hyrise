#ifdef PERSISTENCY_NVRAM

#include "NVManager.h"
#include "NVSimulator.h"

#include <cassert>
#include <cerrno>
#include <cstddef>
#include <cstdio>
#include <cstdlib>
#include <cstring>
#include <mntent.h>
#include <stdexcept>
#include <sys/mman.h>
#include <unistd.h>

#include <pmem.h>
#include <pmemalloc.h>

// ==> PMEM from pmemalloc.h does not work because it uses typeof
#define PPMEM(pmp, ptr_) ((decltype(ptr_))((uintptr_t)pmp + (uintptr_t)ptr_))

// ==> "reverse" PPMEM, converts absolute to relative addresses
#define RPMEM(pmp, ptr_) ((decltype(ptr_))((uintptr_t)ptr_ - (uintptr_t)pmp))

namespace hyrise {
namespace io {

constexpr char   NVM_MOUNTPOINT[]   = NVRAM_MOUNTPOINT;
constexpr char   NVM_TESTFILENAME[] = NVRAM_MOUNTPOINT "/" NVRAM_FILENAME "_test";
constexpr char   NVM_FILENAME[]     = NVRAM_MOUNTPOINT "/" NVRAM_FILENAME;
constexpr size_t NVM_FILESIZE       = 1024ul * 1024ul * NVRAM_FILESIZE;

typedef struct _NVNode {
#ifndef NDEBUG
  uint64_t canary;
#endif
  struct _NVNode *next;
  struct _NVNode *prev;
  NVVectorInfo info;
} NVNode;

typedef struct {
  NVNode *root;
  bool initialized;
} NVStaticInfo;

bool NVManager::isInVolatileMode() const {
  return _nonVolatileMode;
}

NVVectorInfo* NVManager::getOrCreateVectorSpace(uint64_t uuid,
                                                size_t size) {
  NVVectorInfo* vi = NULL;
  if(uuid == 0 && size == 0) {
    throw std::runtime_error("must provide either uuid or num_elements for getOrCreateVectorSpace");

  // known vector, load content
  } else if(uuid > 0) {
    vi = _retrieveVectorInfo(uuid);
    if(!vi) {
      throw std::runtime_error("unable to retrieve vector data");
    }

  // create new vector
  } else if(size > 0) {
    vi = _allocateVectorSpace(size);
    vi->uuid = _generateUUID();
  }

  return vi;
}

NVVectorInfo* NVManager::resizeVectorSpace(NVVectorInfo* vi,
                                           size_t newSize) {
  NVVectorInfo *new_vi = _allocateVectorSpace(newSize);
  memcpy(new_vi + 1, vi + 1, std::min(newSize, vi->allocated));
  new_vi->uuid = vi->uuid;
  persistMemoryArea(new_vi, vi->allocated+sizeof(NVVectorInfo));
  destroyVectorSpace(vi);
  return new_vi;
}

void NVManager::persistMemoryArea(void* start,
                                  size_t len) {
#if NVSIMULATOR_FLUSH_NS > 0
  NVSimulator::persist();
#endif
  assert(start >= _pmp);
  assert((uintptr_t)start + len - (uintptr_t) _pmp < NVM_FILESIZE);
  pmem_persist(start, len, 0);
}

void NVManager::destroyVectorSpace(NVVectorInfo *vi) {
  std::lock_guard<std::mutex> lk(_allocateLock);

  NVStaticInfo* pstatic = (NVStaticInfo*) pmemalloc_static_area(_pmp);
  NVNode* pnode = (NVNode*) ((uintptr_t)RPMEM(_pmp, vi) - offsetof(NVNode, info));
  bool isRoot = (pnode == pstatic->root);

  // if necessary, remap elements of linked list and root pointer
  NVNode *pnext = PPMEM(_pmp, pnode)->next;
  NVNode *pprev = PPMEM(_pmp, pnode)->prev;
  if(pnext)  pmemalloc_onfree(_pmp, pnode, (void**)&PPMEM(_pmp, pnext)->prev, pprev);
  if(pprev)  pmemalloc_onfree(_pmp, pnode, (void**)&PPMEM(_pmp, pprev)->next, pnext);
  if(isRoot) pmemalloc_onfree(_pmp, pnode, (void**)&pstatic->root, pnext);

  // set old NVVectorInfo to reflect deleted state
  vi->uuid = 0;
  vi->size = 0;
  vi->capacity = 0;
  vi->allocated = 0;

  // perform delete
  pmemalloc_free(_pmp, pnode);
}

NVManager::NVManager() {
  _initialize();
}

void NVManager::reset() {
  verifyCanaries();
  _initialize();
}

void NVManager::_initialize() {
  if(!_pmfsMounted()) {
    throw std::runtime_error("PMFS not mounted at " NVRAM_MOUNTPOINT);
  }
  if(!_pmfsWritable()) {
    throw std::runtime_error("no write permission for PMFS");
  }
  _uuidCounter = 1;
  NVStaticInfo *pstatic = NULL;
  if(_pmp) munmap(_pmp, NVM_FILESIZE);
  if((_pmp = pmemalloc_init(NVM_FILENAME, NVM_FILESIZE)) == NULL) {
    printf("error: %s\n", strerror(errno));
    throw std::runtime_error("unable to initialize persistent storage");
  }
  if(!_nonVolatileMode) unlink(NVM_FILENAME); // this will only get deleted on program exit
  if((pstatic = (NVStaticInfo*) pmemalloc_static_area(_pmp)) == NULL) {
    throw std::runtime_error("unable to retrieve static area");
  }
  if(pstatic->initialized == false) {
    pstatic->root = NULL;
    pstatic->initialized = true;
  }
}

uint64_t NVManager::_generateUUID() {
  return _uuidCounter.fetch_add(1);
}

NVVectorInfo* NVManager::_allocateVectorSpace(size_t size) {
  std::lock_guard<std::mutex> lk(_allocateLock);

  NVStaticInfo *pstatic = NULL;
  NVNode *pnode = NULL;

  // get static area where we store the pointer to the root node
  if((pstatic = (NVStaticInfo*) pmemalloc_static_area(_pmp)) == NULL) {
    throw std::runtime_error("unable to retrieve static area");
  }

  // reserve space for a node plus requested size
#ifndef NDEBUG
  if((pnode = (NVNode*) pmemalloc_reserve(_pmp, size + sizeof(NVNode) + sizeof(uint64_t))) == NULL) {
#else
  if((pnode = (NVNode*) pmemalloc_reserve(_pmp, size + sizeof(NVNode))) == NULL) {
#endif
    throw std::runtime_error("unable to allocate memory");
  }

  // set node data
  NVNode* node = PPMEM(_pmp, pnode);
  node->next = pstatic->root;
  node->prev = NULL;
  node->info.size = 0;
  node->info.capacity = 0;
  node->info.allocated = size;

#ifndef NDEBUG
  node->canary = 0xDEADBEEF;
  uint64_t *endcanary = (uint64_t*)((uintptr_t)node + sizeof(NVNode) + size);
  *endcanary = 0xDEADBABE;
#endif

  // mark node to be the new root and activate reserved memory
  if(pstatic->root)
    pmemalloc_onactive(_pmp, pnode, (void**)&PPMEM(_pmp, pstatic->root)->prev, pnode);
  pmemalloc_onactive(_pmp, pnode, (void**)&pstatic->root, pnode);
  pmemalloc_activate(_pmp, pnode);

  return PPMEM(_pmp, &pnode->info);
}

NVVectorInfo* NVManager::_retrieveVectorInfo(uint64_t uuid) const {
  // get static area where we store the pointer to the root node
  const NVStaticInfo* pstatic = NULL;
  if((pstatic = (NVStaticInfo*) pmemalloc_static_area(_pmp)) == NULL) {
    throw std::runtime_error("unable to retrieve static area");
  }
  // find requested vector
  NVNode* pn = PPMEM(_pmp, pstatic->root);
  while(pn) {
    if(pn->info.uuid == uuid) {
      return &pn->info;
    }
    pn = PPMEM(_pmp, pn->next);
  }
  return NULL;
}

bool NVManager::_pmfsMounted() {
  FILE *mtab = NULL;
  struct mntent *part = NULL;
  bool isMounted = false;
  if((mtab = setmntent("/etc/mtab", "r")) == NULL) {
    throw std::runtime_error("unable to acces mount table /etc/mtab");
  }
  while((part = getmntent(mtab)) != NULL) {
    if(part->mnt_dir != NULL && strcmp(part->mnt_dir, NVM_MOUNTPOINT) == 0 && strcmp(part->mnt_type, "pmfs") == 0) {
      isMounted = true;
      break;
    }
  }
  return isMounted;
}

bool NVManager::_pmfsWritable() {
  FILE *fp = NULL;
  bool isWritable = true;
  if((fp = fopen(NVM_TESTFILENAME, "w")) == NULL) {
    if(errno == EACCES) {
      isWritable = false;
    } else {
      throw std::runtime_error("write permission check for PMFS mount failed");
    }
  } else {
    fclose(fp);
    unlink(NVM_TESTFILENAME);
  }
  return isWritable;
}

#ifndef NDEBUG
void NVManager::verifyCanaries() {
  const NVStaticInfo* pstatic = NULL;
  if((pstatic = (NVStaticInfo*) pmemalloc_static_area(_pmp)) == NULL) {
    throw std::runtime_error("unable to retrieve static area");
  }
  // find requested vector
  NVNode* pn = PPMEM(_pmp, pstatic->root);
  while(RPMEM(_pmp, pn)) {
    assert(pn->canary == 0xDEADBEEF);
    assert(*(uint64_t*)((uintptr_t)pn + sizeof(NVNode) + pn->info.allocated) == 0xDEADBABE);
    pn = PPMEM(_pmp, pn->next);
  }
}
#endif

bool NVManager::_instanceCreated, NVManager::_nonVolatileMode;

}
}

#endif
