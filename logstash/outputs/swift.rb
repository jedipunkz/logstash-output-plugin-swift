require "logstash/outputs/base"
require "logstash/namespace"

class LogStash::Outputs::Swift < LogStash::Outputs::Base

  config_name "swift"
  milestone 2

  config :auth_url, :validate => :string, :require => true
  config :auth_user, :validate => :string, :require => true
  config :auth_tenant, :validate => :string, :require => true
  config :auth_api_key, :validate => :string, :require => true
  config :swift_account, :validate => :string, :require => false
  config :swift_container, :validate => :string, :require => true
  config :swift_object_key_format, :validate => :string, :require => false


  public
  def register
    require 'fog'
    require 'zlib'
    require 'time'
    require 'tempfile'
    #require 'open3'
    @conn = Fog::Storage.new :provider => 'OpenStack',
                    :openstack_auth_rul => @auth_url,
                    :openstack_username => @auth_user,
                    :openstack_tenant   => @auth_tenant,
                    :openstack_api_key  => @auth_api_key
    @conn.change_account @swift_account if @swift_account
  end # register

  public
  def receive(event)
    require 'fog'
    require 'zlib'
    require 'time'
    require 'tempfile'
    require 'open3'
    return unless output?(event)

    i = 0
    # @swift_object_key_format = "%{path}%{time_slice}_%{index}.%{file_extension}"
    # swift_path = @swift_object_key_format.gsub(%r(%{[^}]+})) { |expr|
    #         values_for_swift_object_key[expr[2...expr.size-1]]
    swift_path = "testobject"
    @mime_type = 'application/x-gzip'
    tmp = Tempfile.new("swift-")

    begin
      w = Zlib::GzipWriter.new(tmp)
      chunk.write_to(w)
      w.close
      File.open(tmp.path) do |file|
        @conn.put_object(@swift_container, swift_path, file, {:content_type => @mime_type})
      end # File.open
    rescue => e
      @logger.warn("Failed to send event to Swift", :evnet => event, :exception => e,
                    :backtrace => e.backtrace)
    ensure
      w.close rescue nil
    end # begin
  end # def write
end # class LogStash::Outputs::Mongodb
