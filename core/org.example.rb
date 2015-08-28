#!/usr/bin/env ruby

module OrgExample
    include Injector::ModuleInterface
    address 'org.example'
    
    generate ['local.a', 'local.b'] => ['local.a', 'local.x', 'local.b'] do |a,b|
       [a+1, a, b] if a<=b
    end

    filter 'local.x' => 'local.y' do |x|
        x%4!=0
    end

    map 'local.y' => 'local.z' do |y|
        y*3
    end

    reduce ['local.z', 'local.s'=>'local.s0', 'local.p'=>'local.p0'] => ['local.s', 'local.p'] do |z,s,p|
       [s+z, p*z]
    end

    map ['local.s', 'local.p']=>'stdout.written_s' do |s,p|
        puts "Sum is: #{s}"
        puts "Product is: #{p}"
    end
end
