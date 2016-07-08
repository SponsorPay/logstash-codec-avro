# encoding: utf-8
require "open-uri"

require "avro"
require "logstash/codecs/base"
require "logstash/event"
require "logstash/timestamp"
require "logstash/util"
require "schema_registry"
require "schema_registry/client"

# Read serialized Avro records as Logstash events
#
# This plugin is used to serialize Logstash events as
# Avro datums, as well as deserializing Avro datums into
# Logstash events.
#
# ==== Encoding
#
# This codec is for serializing individual Logstash events
# as Avro datums that are Avro binary blobs. It does not encode
# Logstash events into an Avro file.
#
#
# ==== Decoding
#
# This codec is for deserializing individual Avro records. It is not for reading
# Avro files. Avro files have a unique format that must be handled upon input.
#
#
# ==== Usage
# Example usage with Kafka input.
#
# [source,ruby]
# ----------------------------------
# input {
#   kafka {
#     codec => avro {
#         schema_uri => "/tmp/schema.avsc"
#     }
#   }
# }
# filter {
#   ...
# }
# output {
#   ...
# }
# ----------------------------------
class LogStash::Codecs::Avro < LogStash::Codecs::Base
  config_name "avro"


  # schema path to fetch the schema from.
  # This can be a 'http' or 'file' scheme URI
  # example:
  #
  # * http - `http://example.com/schema.avsc`
  # * file - `/path/to/schema.avsc`
  config :schema_uri, :validate => :string
  config :schema_registry_uri, :validate => :string

  def register
    if schema_uri
      @schema = Avro::Schema.parse(Kernel.open(schema_uri).read)
    elsif schema_registry_uri
      @schema_registry = SchemaRegistry::Client.new(schema_registry_uri)
    else
      raise "You must configure either a `schema_uri` or a `schema_registry_uri`"
    end
  end

  def decode(data)
    datum = StringIO.new(data)
    parsed_schema = parsed_schema_for(datum)
    decoder = Avro::IO::BinaryDecoder.new(datum)
    datum_reader = Avro::IO::DatumReader.new(parsed_schema)
    yield LogStash::Event.new(datum_reader.read(decoder))
  end

  def encode(event)
    dw = Avro::IO::DatumWriter.new(@schema)
    buffer = StringIO.new
    encoder = Avro::IO::BinaryEncoder.new(buffer)
    dw.write(event.to_hash, encoder)
    @on_event.call(event, buffer.string)
  end

  private

  def parsed_schema_for(datum)
    magic_byte, schema_id = datum.read(5).unpack('cI>')

    if magic_byte == 0
      schema_from_registry(schema_id)
    else
      datum.rewind
      @schema
    end
  end

  def schema_from_registry(schema_id)
    @cached_schemas ||= Avro::Schema.parse(@schema_registry.schema(schema_id))
  end
end
