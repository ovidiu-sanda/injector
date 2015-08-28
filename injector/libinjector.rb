#!/usr/bin/env ruby

module LibInjector
    refine Array do
        def tail
            self[1..-1]
        end
    end
    refine Hash do
        def map_hash &p
            self.map(&p).reduce({}, &:merge)
        end
    end
end
