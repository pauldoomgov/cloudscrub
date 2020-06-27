RSpec.describe CloudScrub do
  it "has a version number" do
    expect(CloudScrub::VERSION).not_to be nil
  end

  it "initializes" do
    expect(CloudScrub.new).not_to be nil
  end
end
