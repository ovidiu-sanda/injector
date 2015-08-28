#!/usr/bin/env ruby

module Demo3
    include Injector::ModuleInterface

    push 2, 'org.example.a'
    push 7, 'org.example.b'
    push 0, 'org.example.s0'
    push 1, 'org.example.p0'
    pull 'stdout.written_s'
end
