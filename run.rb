#!/usr/bin/env ruby
load 'injector/injector.rb'

injector=Injector.new 8004
injector.load_modules 'modules/org.example.rb', 'modules/config.rb'
injector.wait
