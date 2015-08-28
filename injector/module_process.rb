#!/usr/bin/env ruby

$file_name, port, $key = ARGV
$port=port.to_i
load 'injector/module_interface.rb'
load $file_name
Injector::ModuleInterface::Data[:cmd][:loadend]
Injector::ModuleInterface.wait
