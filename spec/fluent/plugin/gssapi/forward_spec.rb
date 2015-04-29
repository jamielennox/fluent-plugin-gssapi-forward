require 'spec_helper'

describe Fluent::Plugin::Gssapi::Forward do
  it 'has a version number' do
    expect(Fluent::Plugin::Gssapi::Forward::VERSION).not_to be nil
  end

  it 'does something useful' do
    expect(false).to eq(true)
  end
end
