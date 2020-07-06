RSpec.describe CloudScrub do
  it 'has a version number' do
    expect(CloudScrub::VERSION).not_to be nil
  end

  it 'initializes' do
    expect(CloudScrub.new).not_to be nil
  end

  cs = CloudScrub.new

  it 'scrubs messages by gsub literal' do
    expect(cs.scrub_message_by_gsub('this is IT!', '')).to eq('this is IT!')
    expect(cs.scrub_message_by_gsub('this is IT!', 'is')).to eq('th  IT!')
    expect(cs.scrub_message_by_gsub('this is IT!', 'It')).to eq('this is IT!')
  end

  it 'scrubs messages by gsub regex' do
    expect(cs.scrub_message_by_gsub(' Where is my hammer? ', /(^\s+)|(\s+$)/)).to eq('Where is my hammer?')
    expect(cs.scrub_message_by_gsub(' Where is my hammer? ', /er/)).to eq(' Whe is my hamm? ')
    expect(cs.scrub_message_by_gsub(' Where is my hammer? ', /[^\w]+/)).to eq('Whereismyhammer')
  end

  it 'scrubs valid JSON by JSONPath' do
    js = '{"fried": true, "salt": "+++", "secret_ingredients": ["artichoke", "tumeric", "rats"]}'
    expect(cs.scrub_message_by_jsonpaths(js, [])).to eq(js)
    
    jshash = JSON.parse(js)
    jshash.delete('salt')
    jp = ['$..salt'].map { |p| [p, JsonPath.new(p)] }.to_h
    expect(JSON.parse(cs.scrub_message_by_jsonpaths(js, jp))).to eq(jshash)

    jshash.delete('secret_ingredients')
    jp = ['$..salt', '$..secret_ingredients'].map { |p| [p, JsonPath.new(p)] }.to_h
    expect(JSON.parse(cs.scrub_message_by_jsonpaths(js, jp))).to eq(jshash)
  end

  it 'converts epoch ms to ISO8601 time'  do
    expect(cs.stamp_ms_to_iso8601(0)).to eq('1970-01-01T00:00:00.000Z')
    expect(cs.stamp_ms_to_iso8601(1234567891234)).to eq('2009-02-13T23:31:31.233Z')
  end

  it 'converts ISO8601 time to epoch ms' do
    expect(cs.iso8601_to_stamp_ms(Time.at(0).iso8601)).to eq(0)
    # Shortly after the 32 bit Epoch nightmare...
    expect(cs.iso8601_to_stamp_ms('2038-01-19T03:14:10.999Z')).to eq(2147483650999)
  end

  it 'converts human times to epoch ms' do
    expect(cs.timestring_to_stamp_ms('100.10')).to eq(100100)
    expect(cs.timestring_to_stamp_ms('2500-12-31T23:59:59Z')).to eq(16756761599000)
    expect(cs.timestring_to_stamp_ms('2500-12-31 23:59:59Z')).to eq(16756761599000)
    expect(cs.timestring_to_stamp_ms('2500-12-31T18:59:59 -0500')).to eq(16756761599000)
    expect(cs.timestring_to_stamp_ms('2500-12-31T18:59:59.364 -0500')).to eq(16756761599364)
  end

  it 'serializes events' do
    expect(cs.serialize_event(
      16756761599000,
      '/var/garbage/log',
      'junkhost',
      'WARNING: Garbage')
    ).to eq("{\"@timestamp\":\"2500-12-31T23:59:59.000Z\",\"type\":\"cw_/var/garbage/log\",\"host\":{\"name\":\"junkhost\"},\"events\":\"WARNING: Garbage\"}\n")
  end

  it 'deserializes events' do
    expect(cs.deserialize_event(
      "{\"@timestamp\":\"2500-12-31T23:59:59.000Z\",\"type\":\"cw_/var/garbage/log\",\"host\":{\"name\":\"junkhost\"},\"events\":\"{\\\"nested\\\": true}\"}\n"
    )).to eq([
      "2500-12-31T23:59:59.000Z",
      "/var/garbage/log",
      "junkhost",
      "{\"nested\": true}"
    ])
  end
end
