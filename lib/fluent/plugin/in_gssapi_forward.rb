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

require 'base64'

module Fluent
  class GSSAPIForwardInput < Input
    Fluent::Plugin.register_input('gssapi_forward', self)

    def initialize
      super

      require 'gssapi'
      require 'fluent/plugin/socket_util'

      @u = MessagePack::Unpacker.new
    end

    config_param :port, :integer, :default => DEFAULT_LISTEN_PORT
    config_param :bind, :string, :default => '0.0.0.0'
    config_param :backlog, :integer, :default => nil
    config_param :blocking_timeout, :time, :default => 0.5

    config_param :host, :string
    config_param :service, :string
    config_param :keytab, :string, :default => nil

    def configure(conf)
      super
    end

    def start
      super

      @lsock = Coolio::TCPServer.new(@bind, @port,
                                     GSSAPISocket, log, method(:on_message),
                                     host, service, keytab)
      @lsock.listen(@backlog) unless @backlog.nil?

      log.info "GSSAPI socket listening on #{@bind}:#{@port}"

      @loop = Coolio::Loop.new
      @loop.attach(@lsock)

      @thread = Thread.new(&method(:run))
    end

    def run
      @loop.run @blocking_timeout
    rescue => e
      log.error "unexpected error", :error => e, :error_class => e.class
      log.error_backtrace
    end

  private

    def on_message(data, sock)
      msg = MessagePack.unpack(data)

      tag = msg[1].to_s
      entries = msg[2]

      es = MessagePackEventStream.new(entries)
      router.emit_stream(tag, es)
    end

    #
    # A very simple protocol. Each message consists of a 32 bit number
    # representing the number of preceeding bytes. These may not come in on
    # network boundaries and so we buffer incoming data until this condition is met.
    # NOTE: I was going to just use msgpack for this however it doesn't support
    # sending binary data, see https://github.com/msgpack/msgpack-ruby/issues/44
    #
    class Buffer

      def initialize
        @buffer = String.new
      end

      def feed(data)
        @buffer << data
      end

      def next_value
        # the references to 4 in this function are because the length of the
        # initial integer is 4 bytes.
        return nil if @buffer.length < 4

        required = @buffer.unpack("N")[0]
        return nil if @buffer.length < required + 4

        val = @buffer.byteslice(4, required)
        @buffer = @buffer.byteslice((required + 4)..-1)

        val
      end

      def each(&block)
        while v = next_value
          block.call v
        end
      end

      def feed_each(data, &block)
        feed(data)
        each(&block)
      end

    end

    class GSSAPISocket < Coolio::TCPSocket

      def initialize(io, log, on_message, host, service, keytab)
        super io

        @log = log
        @log.info "new GSSAPI connection from #{remote_addr}:#{remote_port}"

        @b = Buffer.new
        @on_message = on_message

        @host = host
        @service = service
        @keytab = keytab
      end

      #
      # The first value down the socket is the GSSAPI context object to
      # establish the handhake. Once this has been established futher messages can be
      # offloaded to the inner fluentd mechanism.
      #
      def on_read(data)
        @b.feed_each(data) do |msg|
          if @gssapi
            unwrapped = @gssapi.unwrap_message(msg)
            @on_message.call unwrapped, self
          else
            g = GSSAPI::Simple.new @host, @service, @keytab
            g.acquire_credentials
            tok = g.accept_context msg
            write "#{Base64.strict_encode64(tok)}\n"
            @log.trace "GSSAPI handshake complete"
            @gssapi = g
          end
        end

      rescue GSSAPI::GssApiError => e
        @log.error "GSSAPI Failure: #{e.message}"
        close
        raise
      end

    end

  end
end
