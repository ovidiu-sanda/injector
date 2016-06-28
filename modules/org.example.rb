#!/usr/bin/env ruby

module OrgExample
    include Injector::ModuleInterface
    using LibInjector
    address 'org.example'

    enumerate 'local.v' => 'local.x'
    #or
    #generate 'local.v' => ['local.v', 'local.x'] do |v|
    #    [v.tail, v.head] if v.any?
    #end

    filter 'local.x' => 'local.x2' do |x|
        x%4!=0
    end

    map 'local.x2' => 'local.y' do |x2|
        x2*3
    end

    reduce ['local.y', 'local.s'=>'local.s0', 'local.p'=>'local.p0'] => ['local.s', 'local.p'] do |y,s,p|
       [s+y, p*y]
    end

    map ['local.s', 'local.p']=>'stdout.written_s' do |s,p|
        puts "Sum is: #{s}"
        puts "Product is: #{p}"
    end
end
