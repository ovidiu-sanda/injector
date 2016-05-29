#!/usr/bin/env ruby
require 'socket'
require 'yaml'
require 'logger'
require 'pry'
load 'injector/libinjector.rb'

class Injector
    using LibInjector

    def initialize port
        @port=port
        @server=TCPServer.new '127.0.0.1', @port
        @key_chars=[*('a'..'z'), *('A'..'Z'), *('0'..'9')]
        @connected_modules_raw=[]
        @connected_modules=[]
        @pids=[]
        @client_addresses={}
        at_exit{@pids.each{|pid| Process.kill(9, pid)}}
        Thread.abort_on_exception=true
        @proc_units={}
        @capability={}
        @sub_cap={}
        @rsub_cap={}
        @rsub_cap.default=[]
        @pushed_data={}
        @pull_addresses=[]
        @unit_method=[:map, :filter, :generate, :reduce].map{|m| [m, method(m)]}.to_h
        @exec_queue=[]
        @reducers=[]
        @logger=Logger.new STDOUT
        @logger.formatter=proc do |severity, datetime, progname, message|
            "[#{severity}] #{datetime.hour}:#{datetime.min}:#{datetime.sec} #{message}\n"
        end
        @logger.level=Logger::INFO
    end

    def all_modules_loaded
        @keys.sort == @connected_modules.sort
    end

    def load_modules *mods
        @mods=mods
        @keys=[]
        @logger.info 'Loading modules...'
        mods.each do |mod|
            key=(1..20).map{@key_chars.sample}.join
            @keys<<key
            @logger.debug "Generating key #{key} for module #{mod} and spawning process"
            pid=spawn 'ruby', 'injector/module_process.rb', mod, @port.to_s, key
            @pids<<pid
            @client_addresses[key]=mod
            @logger.info "Spawned process with pid #{pid} for module #{mod}"
        end
        ts=[]
        @clients={}
        main_thread=Thread.new do
            loop do
                ts<<Thread.new(@server.accept) do |client|
                    data=""
                    loop do
                        data_line=client.readline
                        break if data_line.chomp=='loadend'
                        data<<data_line
                    end
    
                    key=data.lines.first.chomp
                    if @keys.include? key
                        addr=@client_addresses[key]
                        @logger.info "New connected module: #{addr}"
                        @logger.debug data
                        @clients[addr]=client
                        @connected_modules_raw<<data
                        @connected_modules<<key
                        raw_units=data.split(/^---\n/).tail.map{|x| YAML.load x}
                        push_units=raw_units.select{|unit| unit[1]==:push}
                        address_unit=raw_units.select{|unit| unit[1]==:address}.last
                        address=(address_unit[2].first if address_unit) || ''
                        @pushed_data.merge! push_units.map{|unit| {unit[2].last.sub(/^local\./, address+'.')=>unit[2].first}}.reduce({}, &:merge)
                        pull_units=raw_units.select{|unit| unit[1]==:pull}
                        @pull_addresses+=pull_units.map{|p| p[2].first.sub(/^local\./, address+'.')}
                        @proc_units[addr]=raw_units.select{|unit| unit.last}.map do |unit|
                            inputs_raw=unit[2].first.keys.first
                            if inputs_raw.kind_of? Array
                                inputs=inputs_raw.map do |x|
                                    if x.kind_of? Hash
                                        x.map_hash{|a1,a2| {a1.sub(/^local\./, address+'.')=>a2.sub(/^local\./, address+'.')}}
                                    else
                                        x.sub(/^local\./, address+'.')
                                    end
                                end
                            else
                                inputs=inputs_raw.sub(/^local\./,address+ '.')
                            end
                            outputs_raw=unit[2].first.values.first
                            if outputs_raw.kind_of? Array
                                outputs=outputs_raw.map do |x|
                                    x.sub(/^local\./, address+'.')
                                end
                            else
                                outputs=outputs_raw.sub(/^local\./, address+'.')
                            end
                            {type:unit[1], inputs:inputs, outputs:outputs, data:{}, proc:proc do |*args|
                                exec_proc addr, unit[0], *args
                            end}
                        end
                        @reducers+=@proc_units[addr].select{|unit| unit[:type]==:reduce}
                        @proc_units[addr].each do |unit|
                            [unit[:outputs]].flatten.each do |output|
                                @capability[output]=unit
                            end
                        end
                        main_thread.kill if all_modules_loaded
                    else
                        @logger.error "Unknown client key: #{key}"
                        client.close
                    end
                end
            end
        end
        Thread.new do
            sleep 5
            if !all_modules_loaded
                @logger.fatal "Error: not all modules connected!"
                @logger.fatal @keys
                @logger.fatal "Connected:"
                @logger.fatal @connected_modules
                binding.pry
            end
        end
        main_thread.join
        @logger.info "Application loaded successfully!"
    end

    def exec_proc addr, n, *args
        client=@clients[addr]
        client.send YAML.dump(['exec', n, args]), 0
        client.send "dataend\n", 0
        data=""
        loop do
            data_line=client.readline
            break if data_line.chomp=='dataend'
            data<<data_line
        end
        YAML.load(data)
    end

    def get_output addr, outputs, data
        if outputs.respond_to? :find_index
            data[outputs.find_index(addr)]
        elsif addr==outputs
            data
        else
            raise "Address #{addr} missing from outputs #{outputs}!"
        end
    end

    def get_outputs unit, data
        if unit[:outputs].respond_to? :zip
            unit[:outputs].zip(data).to_h
        else
            {unit[:outputs]=>data}
        end
    end

    def pushed addr
        @pushed_data[addr]
    end

    def init_cap *addrs
        addrs.each do |addr|
            value=pushed(addr)
            unless value
                unit=@sub_cap[addr]=@capability[addr]
                [unit[:inputs]].flatten.each do |iaddr|
                    if iaddr.kind_of? Hash
                        iaddr.values.each do |iaddr2|
                            if @rsub_cap[iaddr2].empty?
                                @rsub_cap[iaddr2]=[unit]
                            else
                                @rsub_cap[iaddr2] <<unit unless @rsub_cap[iaddr2].include? unit
                            end
                        end
                    else
                        if @rsub_cap[iaddr].empty?
                            @rsub_cap[iaddr]=[unit]
                        else
                            @rsub_cap[iaddr] <<unit unless @rsub_cap[iaddr].include? unit
                        end
                    end
                end
                init_cap *[*[unit[:inputs]].flatten.map{|addr| addr.kind_of?(Hash) ? addr.values : addr}.flatten]
            end
        end
    end

    def push value, addr
        #put data in each unit input
        @rsub_cap[addr].each do |unit|
            if unit[:inputs].include? addr
                unit[:data][addr]=value
            elsif unit[:inputs].last.respond_to?(:rassoc) && unit[:inputs].last.rassoc(addr)
                unit[:data][unit[:inputs].last.rassoc(addr).first]=value
            else
                raise "No address #{addr} in unit with outputs #{unit[:outputs]}"
            end
        end
        #check if output units have all their inputs and outputs ready
        @rsub_cap[addr].each{|unit| check_unit unit}
    end

    def map unit
        #execute proc and push data
        data=unit[:proc][*[unit[:inputs]].flatten.map{|addr| unit[:data][addr]}]
        outputs=get_outputs unit, data
        #remove data from unit
        unit[:data]={}
        outputs.each{|addr, value| push value, addr}
        #check if input units have all their inputs and outputs ready
        [unit[:inputs]].flatten.each{|addr| check_unit @sub_cap[addr] if @sub_cap[addr]}
    end

    def filter unit
        #execute proc and push data if condition applies
        data=unit[:proc][*[unit[:inputs]].flatten.map{|addr| unit[:data][addr]}]
        #remove data from unit
        value=unit[:data].values.first
        unit[:data]={}
        if data
            push value, unit[:outputs]
        end
        #check if input units have all their inputs and outputs ready
        [unit[:inputs]].flatten.each{|addr| check_unit @sub_cap[addr] if @sub_cap[addr]}
    end

    def reject unit
        #execute proc and push data if condition applies
        data=unit[:proc][*[unit[:inputs]].flatten.map{|addr| unit[:data][addr]}]
        #remove data from unit
        value=unit[:data].values.first
        unit[:data]={}
        unless data
            push value, unit[:outputs]
        end
        #check if input units have all their inputs and outputs ready
        [unit[:inputs]].flatten.each{|addr| check_unit @sub_cap[addr] if @sub_cap[addr]}
    end

    def generate unit
        #no nested generators for now
        #execute proc and push data if not nil/false
        data=unit[:proc][*[unit[:inputs]].flatten.map{|addr| unit[:data][addr]}]
        #remove data from unit
        unit[:data]={}
        if data
            outputs=get_outputs unit, data
            #recursive streams are pushed back to the generator
            rec_s=[unit[:inputs]].flatten
            rec_s.each{|addr| unit[:data][addr]=outputs[addr]}
            #non-recursive streams are pushed forward
            nrec_s=unit[:outputs] - rec_s
            nrec_s.each{|addr| push outputs[addr], addr}
        else
            unit[:finished]=true
        end
    end

    def reduce unit
        #execute proc
        data=unit[:proc][*[unit[:inputs]].flatten.map{|addr| addr.kind_of?(Hash) ? addr.keys : addr}.flatten.map{|addr| unit[:data][addr]}]
        #remove data from unit
        unit[:data]={}
        #push back data
        #no need to check if same unit is ready again in single threaded environment
        outputs=get_outputs unit, data
        outputs.each{|addr, value| unit[:data][addr]=value}
    end

    def add_unit unit
        #schedule execution
        @logger.debug "Adding unit for execution with outputs #{unit[:outputs].to_s}"
        @exec_queue<<unit
    end

    def check_unit unit
        #check inputs and outputs if ready
        #if ready, add to queue/thread pool
        inputs_ready=[unit[:inputs]].flatten.map{|addr| addr.kind_of?(Hash) ? addr.keys : addr}.flatten.sort==unit[:data].keys.sort
        if inputs_ready
            if unit[:type]==:generate
                outputs_addrs=unit[:outputs] - [unit[:inputs]].flatten
            else
                outputs_addrs=[unit[:outputs]].flatten
            end
            outputs_ready=outputs_addrs.all? do |addr|
                @rsub_cap[addr].none?{|u| u[:data][addr]}
            end
            add_unit unit if outputs_ready
        end
        #
        #race condition: how to prevent same compution to be added twice
        #also, how to make exec_proc concurrent? we can add a mutex for safety,
        #but can we handle concurrency within the same module?
        #a mutex will destroy all concurrency, we need a mutex per client
        #single threaded for now
        #
    end

    def push_reducer_outputs unit
        #we check if the addresses in the hash have data available and push it
        unit[:inputs].last.keys.each do |addr|
            value=unit[:data][addr]
            if value
                push value, addr
                unit[:finished]=true
            end
        end
    end

    def run
        init_cap *@pull_addresses
        #first we push all data to each unit input
        #we either ensure that only pushes are to be executed in the beginning or we check other units,
        #such as enumerate 1,3
        #we shift each unit from queue until no more units
        #we execute @unit_method[type] for push, map, filter etc.
        #input data to pulls is not stored, e.g. executing a pull just removes its input data
        #special treatment for push for simplicity
        @pushed_data.each{|addr, data| push data, addr}
        loop do
            loop do
                break if @exec_queue.empty?
                unit=@exec_queue.shift
                @unit_method[unit[:type]][unit]
            end
            #next, we prepare reducer data and loop with the above
            @reducers.reject{|r| r[:finished]}.each{|r| push_reducer_outputs r}
            break if @exec_queue.empty?
        end
    end

    def wait
        @logger.info "Running..."
        begin
            run
        rescue=>e
            @logger.fatal "Error: #{e.message}"
            @logger.fatal e.backtrace.join("\n")
            @error=e
        ensure
            binding.pry
        end
    end
end
