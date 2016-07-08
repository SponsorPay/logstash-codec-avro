# encoding: utf-8
require "logstash/devutils/rspec/spec_helper"
require 'logstash/codecs/avro'

describe LogStash::Codecs::Avro do
  def encoded_message
    schema = Avro::Schema.parse(schema_string)
    dw = Avro::IO::DatumWriter.new(schema)
    buffer = StringIO.new
    encoder = Avro::IO::BinaryEncoder.new(buffer)
    dw.write(test_event.to_hash, encoder)
    buffer.string
  end

  let(:schema_string) do
    <<-SCHEMA
     {
       "type": "record",
       "name": "Test",
       "fields": [
         {"name": "foo", "type": ["null", "string"]},
         {"name": "bar", "type": "int"}
       ]
     }
   SCHEMA
  end

  let(:test_event) { LogStash::Event.new({"foo" => "hello", "bar" => 10}) }

  context 'avro config uses schema_uri' do
    before do
      allow(Kernel).to receive(:open).with(avro_config['schema_uri']).and_return(StringIO.new(schema_string))
    end

    let(:avro_config) { Hash['schema_uri', 'path/to/schemas'] }

    describe "#decode" do
      it "should return an LogStash::Event from avro data" do
        described_class.new(avro_config).decode(encoded_message) do |event|
          insist { event.is_a? LogStash::Event }
          insist { event["foo"] } == test_event["foo"]
          insist { event["bar"] } == test_event["bar"]
        end
      end
    end

    describe "#encode" do
      it "should return avro data from a LogStash::Event" do
        got_event = false
        logstash_event = described_class.new(avro_config)

        logstash_event.on_event do |event, data|
          schema = Avro::Schema.parse(schema_string)
          datum = StringIO.new(data)
          decoder = Avro::IO::BinaryDecoder.new(datum)
          datum_reader = Avro::IO::DatumReader.new(schema)
          record = datum_reader.read(decoder)

          insist { record["foo"] } == test_event["foo"]
          insist { record["bar"] } == test_event["bar"]
          insist { event.is_a? LogStash::Event }
          got_event = true
        end

        logstash_event.encode(test_event)
        insist { got_event }
      end
    end
  end

  context "avro config uses schema_registry_uri" do
    let(:avro_config) { Hash['schema_registry_uri','http://registry.example.com:8080'] }

    describe '#decode' do
      it "should return a LogStash::Event from avro data" do
        schema_registry_double = instance_double(SchemaRegistry::Client)

        expect(schema_registry_double).to receive(:schema).with(1234).and_return(schema_string)
        allow(SchemaRegistry::Client).to receive(:new).with(avro_config['schema_registry_uri']).
                                                       and_return(schema_registry_double)

        magic_byte_plus_schema_id = [0, 1234].pack('cI>')

        described_class.new(avro_config).decode([magic_byte_plus_schema_id, encoded_message].join) do |event|
          insist { event.is_a? LogStash::Event }
          insist { event["foo"] } == test_event["foo"]
          insist { event["bar"] } == test_event["bar"]
        end
      end
    end
  end

  context "avro config uses neither schema_uri nor schema_registry_uri" do
    it "raises error at initialization" do
      expect { described_class.new({}) }.
        to raise_error(RuntimeError, "You must configure either a `schema_uri` or a `schema_registry_uri`")
    end
  end
end
