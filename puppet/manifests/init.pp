class roles_rw_neo4j {

  $configParameters = hiera('configParameters','')

  class { "go_service_profile" :
    service_module => $module_name,
    service_name => 'roles-rw-neo4j',
    configParameters => $configParameters
  }
}
