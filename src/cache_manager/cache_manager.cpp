#include "cache_manager/service/CacheManagerServer.h"
#include "common/app/TwoPhaseApplication.h"

int main(int argc, char *argv[]) {
  using namespace hf3fs;
  return TwoPhaseApplication<cache_manager::CacheManagerServer>().run(argc, argv);
}
