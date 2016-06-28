#!/usr/bin/env ruby

module Config
    include Injector::ModuleInterface

    push InjectorEnum.new([2,3,4,5,6,7]), 'org.example.v'
    push 0, 'org.example.s0'
    push 1, 'org.example.p0'
    pull 'stdout.written_s'
end
