#!/usr/bin/env ruby
require 'socket'
require 'yaml'
require 'logger'
load 'injector/libinjector.rb'

class Injector
    module ModuleInterface
        Data={units:[]}
        def method_missing name, *args, &l
            Data[:units]<<[name, args, l]
            Data[:client][name, args, l]
        end

        def self.included mod
            logger=Logger.new STDOUT
            logger.formatter=proc do |severity, datetime, progname, message|
                "[#{severity}] #{datetime.hour}:#{datetime.min}:#{datetime.sec} #{message}\n"
            end
            logger.level=Logger::INFO
            logger.debug "Connecting to injector on port #$port with key #$key..."
            client=TCPSocket.open '127.0.0.1', $port.to_i
            client.send $key+"\n", 0
            n=0
            Data[:client]=lambda do |name, args, l|
                client.send YAML.dump([n, name, args, !l.nil?]), 0
                n=n+1
            end
            Data[:cmd]=lambda do |data|
                client.send data.to_s+"\n", 0
            end
            Data[:rawclient]=client
            mod.define_singleton_method('method_missing',instance_method('method_missing'))
        end

        def self.wait
            logger=Logger.new STDOUT
            logger.formatter=proc do |severity, datetime, progname, message|
                "[#{severity}] #{datetime.hour}:#{datetime.min}:#{datetime.sec} #{message}\n"
            end
            logger.level=Logger::INFO
            loop do
                raw_data=''
                loop do
                    data_line=Data[:rawclient].gets
                    break if data_line.chomp=='dataend'
                    raw_data<<data_line
                end
                if !raw_data || raw_data.empty?
                    logger.error 'Dead connection?'
                    #exit 1
                    sleep 1
                    redo
                end
                logger.debug raw_data
                data=YAML.load(raw_data)
                if data.first=='exec'
                     Data[:rawclient].send YAML.dump(Data[:units][data[1]].last[*data[2]]), 0
                     Data[:cmd][:dataend]
                end
            end
        end
    end
end
