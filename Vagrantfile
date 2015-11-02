# -*- mode: ruby -*-
# vi: set ft=ruby :

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
  config.vm.provision "ansible" do |ansible|
    ansible.sudo=true
    ansible.playbook = "provision.yaml"
  end
end
