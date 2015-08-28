#!/usr/bin/env ruby
load 'injector/injector.rb'

injector=Injector.new 8004
injector.load_modules 'core/org.example.rb', 'core/config.rb'
injector.wait
