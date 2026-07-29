target_add_lib(cache-manager cache-origin core-app core-user core-service cache-manager-fbs meta-client mgmtd-client
               storage-client)
target_add_bin(cache_manager_main "cache_manager.cpp" cache-manager)
