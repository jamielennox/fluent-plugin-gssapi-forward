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

      require 'base64'
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

        service = e['service']
        name = e['name'] || "#{host}:#{port}"
        keytab = e['keytab']

        log.info "GSSAPI forwarding server configured '#{name}'", :host => host, :port => port, :service => service
        ServerConfig.new host, port, service, keytab, log
      end
    end

    # def start
    #   super

    #   @loop = Coolio::Loop.new
    #   @nodes.each {|n| @loop.attach n.sock }
    #   @thread = Thread.new(&method(:run))
    # end

    # def shutdown
    #   super
    #   @loop.watchers.each {|w| w.detach }
    #   @loop.stop
    #   @thread.join
    # end

    # def run
    #   @loop.run
    # rescue => e
    #   log.error "unexpected error", :error => e, :error_class => e.class
    #   log.error_backtrace
    # end

    def write_objects(tag, chunk)
      log.info "writing", :tag => tag, :chunk => chunk
      return if chunk.empty?

      @nodes.each do |node|
        log.info "sending to #{node}"
        node.write tag, chunk
      end

    rescue => e
      log.error "couldn't send", :error => e, :error_class => e.class
      log.error_backtrace
    end

  private

    class ServerConfig

      attr_reader :port

      def initialize host, port, service, keytab, log
        @host = host
        @port = port
        @service = service
	@log = log
        @keytab = keytab
      end

      def write tag, chunk
        c = chunk.read rescue chunk

        msg = gssapi.wrap_message [0, tag, c].to_msgpack
        sock.write [ 0xc6, msg.length ].pack("NN")
        sock.write msg

      rescue GSSAPI::GssApiError => e
        @log.warn "GSSAPI failure: #{e.message}"
        sock.close
        raise
      end

      def sock
        return @sock unless @sock.nil?

        @sock = TCPSocket.new resolved, port

        # opt = [1, @sender.send_timeout.to_i].pack('I!I!')  # { int l_onoff; int l_linger; }
        # s.setsockopt(Socket::SOL_SOCKET, Socket::SO_LINGER, opt)

        # opt = [@sender.send_timeout.to_i, 0].pack('L!L!')  # struct timeval
        # s.setsockopt(Socket::SOL_SOCKET, Socket::SO_SNDTIMEO, opt)
        @sock
      end

      def gssapi
        return @gssapi unless @gssapi.nil?

        @gssapi = GSSAPI::Simple.new @host, @service, @keytab
        tok = @gssapi.init_context

        sock.write [ 0xc6, tok.length ].pack('NN')
        sock.write tok

        data = sock.gets.chomp

        ctx = @gssapi.init_context Base64.strict_decode64(data)
        raise "Couldn't establish GSS Connection" unless ctx

        @log.warn "GSSAPI Handshake complete"

        @gssapi
      end

      def resolved
        sockaddr = Socket.pack_sockaddr_in @port, @host
        _, resolved_host = Socket.unpack_sockaddr_in sockaddr
        resolved_host
      end
    end

  end
end

