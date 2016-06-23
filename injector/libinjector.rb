#!/usr/bin/env ruby

class InjectorEnum
    def initialize v, i=0
        @v=v
        @i=i
    end

    def head
        @i<@v.size ? @v[@i] : nil
    end

    def tail
        InjectorEnum.new @v, @i<@v.size ? @i+1 : @i
    end

    def any?
        @i<@v.size
    end
end

module LibInjector
    refine Array do
        def tail
            self[1..-1]
        end

        def to_enum_i
            InjectorEnum.new self
        end
    end
    refine Hash do
        def map_hash &p
            self.map(&p).reduce({}, &:merge)
        end
    end
end
