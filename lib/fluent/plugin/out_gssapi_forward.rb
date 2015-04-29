# Licensed under the Apache License, Version 2.0 (the "License"); you may
# not use this file except in compliance with the License. You may obtain
# a copy of the License at
#
#      http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
# WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
# License for the specific language governing permissions and limitations
# under the License.

module Fluent
  class GSSAPIForwardOutput < ObjectBufferedOutput
    Fluent::Plugin.register_output('gssapi_forward', self)

    def initialize
      super

      require 'gssapi'
      require 'fluent/plugin/socket_util'
    end

    attr_reader :nodes

    def configure(conf)
      super

      @nodes = conf.elements.map do |e|
        next if e.name != "server"

        host = e['host']
        port = e['port']
        port = port ? port.to_i : DEFAULT_LISTEN_PORT

        weight = e['weight']
        weight = weight ? weight.to_i : 60

        name = e['name'] or "#{host}:#{port}"

        log.info "New forwarding server configured '#{name}'", :name => name
        NodeConfig :name => name, :host => host, :port => port, :weight => weight
      end
    end

    def start
      super

      @loop = Coolio::Loop.new
      @thread = Thread.new(&method(:run))
    end

    def shutdown
      @loop.watchers.each {|w| w.detach }
      @loop.stop
      @thread.join
    end

    def run
      @loop.run
    rescue => e
      log.error "unexpected error", :error => e, :error_class => e.class
      log.error_backtrace
    end

  private

    NodeConfig = Struct.new("NodeConfig", :name, :host, :port, :weight)

  end
end

