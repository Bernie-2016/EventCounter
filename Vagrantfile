# -*- mode: ruby -*-
# vi: set ft=ruby :

require 'securerandom'

# Vagrantfile API/syntax version. Don't touch unless you know what you're doing!
VAGRANTFILE_API_VERSION = "2"

# Basic machine config
Vagrant.configure(VAGRANTFILE_API_VERSION) do |config|
  config.vm.box = "ubuntu/trusty64"
  config.vm.network :forwarded_port, guest: 5000, host: 7000
  config.vm.provider "virtualbox" do |v|
    v.cpus = 1
    v.memory = 256
  end
end

Vagrant.configure(VAGRANTFILE_API_VERSION) do |config|
  appname = ( ENV["HEROKU_APP_NAME"] or 'event-counter-'+SecureRandom.uuid )
  config.vm.provision "ansible" do |ansible|
    ansible.extra_vars = {
      "heroku_email" => ENV["HEROKU_EMAIL"],
      "heroku_api_key"  => ENV["HEROKU_API_KEY"],
      "heroku_appname"  => appname,
      "bsd_host" => ENV['BSDHOST'],
      "bsd_id" => ENV['BSDID'],
      "bsd_secret" => ENV['BSDSECRET'],
    }
    ansible.playbook = "provision.yaml"
  end
end
