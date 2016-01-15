class roles_rw_neo4j {

  $binary_name = "roles-rw-neo4j"
  $install_dir = "/usr/local/$binary_name"
  $binary_file = "$install_dir/$binary_name"
  $log_dir = "/var/log/apps"

  class { 'common_pp_up': }
  class { "${module_name}::monitoring": }
  class { "${module_name}::supervisord": }

  user { $binary_name:
    ensure    => present,
  }

  file {
    $install_dir:
      mode    => "0664",
      ensure  => directory;

    $binary_file:
      ensure  => present,
      source  => "puppet:///modules/$module_name/$binary_name",
      mode    => "0755",
      require => File[$install_dir];

    $log_dir:
      ensure  => directory,
      mode    => "0664"
  }

  exec { 'restart_app':
    command     => "supervisorctl restart $binary_name",
    path        => "/usr/bin:/usr/sbin:/bin",
    subscribe   => [
      File[$binary_file],
      Class["${module_name}::supervisord"]
    ],
    refreshonly => true
  }
}
