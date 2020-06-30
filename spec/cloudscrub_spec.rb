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

  it 'converts epoch ms to time'  do
    expect(cs.parse_timestamp_ms(0)).to eq(Time.at(0).utc)
    expect(cs.parse_timestamp_ms(1234567891234).iso8601).to eq('2009-02-13T23:31:31Z')
  end

  it 'converts time to epoch ms' do
    expect(cs.time_to_stamp_ms(Time.at(0))).to eq(0)
    # Shortly after the 32 bit Epoch nightmare...
    expect(cs.time_to_stamp_ms(Time.at(2_147_483_648_999))).to eq(2_147_483_648_999_000)
  end

  it 'converts human times to epoch ms' do
    expect(cs.timestring_to_stamp_ms('100.10')).to eq(100100)
    expect(cs.timestring_to_stamp_ms('2500-12-31T23:59:59Z')).to eq(16756761599000)
    expect(cs.timestring_to_stamp_ms('2500-12-31 23:59:59Z')).to eq(16756761599000)
    expect(cs.timestring_to_stamp_ms('2500-12-31T18:59:59 -0500')).to eq(16756761599000)
    expect(cs.timestring_to_stamp_ms('2500-12-31T18:59:59.364 -0500')).to eq(16756761599364)
  end

  it 'serializes events' do
    expect(cs.serialize_event(16756761599000, '/var/garbage/log', 'WARNING: Garbage')).to eq("16756761599000 /var/garbage/log WARNING: Garbage\n")
    expect(cs.serialize_event(16756761599000, '/var/garbage/log', ' WARNING: Garbage!')).to eq("16756761599000 /var/garbage/log  WARNING: Garbage!\n")
  end

  it 'deserializes events' do
    expect(cs.deserialize_event("16756761599000 /var/garbage/log WARNING: Garbage\n")).to eq([16756761599000, '/var/garbage/log', 'WARNING: Garbage'])
    expect(cs.deserialize_event('16756761599231 /var/garbage/logz! {"WARNING": "Garbage", "types": ["wrappers", "bottles", "OTHER"]} ')).to eq([16756761599231, '/var/garbage/logz!', '{"WARNING": "Garbage", "types": ["wrappers", "bottles", "OTHER"]} '])
  end
end
