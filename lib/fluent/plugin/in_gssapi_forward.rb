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
  class GSSAPIForwardInput < Input
    Fluent::Plugin.register_input('gssapi_forward', self)

    def initialize
      super

      require 'gssapi'
      require 'fluent/plugin/socket_util'
    end

    config_param :port, :integer, :default => DEFAULT_LISTEN_PORT
    config_param :bind, :string, :default => '0.0.0.0'
    config_param :backlog, :integer, :default => nil
    config_param :blocking_timeout, :time, :default => 0.5

    # SO_LINGER 0 to send RST rather than FIN to avoid lots of connections sitting in TIME_WAIT at src
    config_param :linger_timeout, :integer, :default => 0

    def configure(conf)
      super
    end

    def start
      super

      log.info "listening fluent socket on #{@bind}:#{@port}"

      @lsock = Coolio::TCPServer.new(@bind, @port, Handler, @linger_timeout, log, method(:on_message))
      @lsock.listen(@backlog) unless @backlog.nil?

      @loop = Coolio::Loop.new
      @loop.attach(@lsock)

      @thread = Thread.new(&method(:run))
    end

    def run
      @loop.run(@blocking_timeout)
    rescue => e
      log.error "unexpected error", :error => e, :error_class => e.class
      log.error_backtrace
    end

  private

    def on_message(msg, chunk_size, source)

    end

    class Handler < Coolio::Socket

    end

  end
end

