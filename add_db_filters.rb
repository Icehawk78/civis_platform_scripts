require 'json'
require 'pg'
require 'net/http'
require 'net/https'
require 'uri'

def get_https(url)
  uri = URI.parse(url)
  https = Net::HTTP.new(uri.host, uri.port)
  https.use_ssl = true
end

def get_url(url)
  https = get_https(url)
  req = Net::HTTP::GET.new(https.path)
  https.request(req).body
end

def post_url(url, data)
  https = get_https(url)
  req = Net::HTTP::POST.new(https.path)
  req.set_form_data(data)
  https.request(req).body
end

begin
  @json = JSON.parse(get_url(ENV['json_config_url']))
  base_id = @json['other_seeds']['features'].map{|f| f['id']}.compact.max + 1
  root_feature = @json['other_seeds']['features'].find{|f| f['id'] == ENV['root_id']} if ENV['root_id']
  if root_feature.nil?
    root_feature = {
      'id': base_id,
      'name': ENV['root_name'] || 'Default Root Name',
      'root_id': nil,
      'parent_id': nil,
      'variable_type_id': (ENV['is_multiselect'] ? 5 : 3),
      'is_cuttable': true,
      'value': nil
    }
    @json['other_seeds']['features'] << root_feature
    base_id += 1
  end
  feature_template = {
    id: -1,
    name: '',
    value: '',
    parent_id: root_feature['id'],
    root_id: root_feature['id'],
    variable_type_id: root_feature['variable_type_id'],
    is_cuttable: root_feature['is_cuttable']
  }

  @connection = PG::connect(
    host: ENV['DATBASE_HOST'],
    port: ENV['DATABASE_PORT'],
    dbname: ENV['DATABASE_DATABASE'],
    user: ENV['DATABASE_CREDENTIAL_USERNAME'],
    password: ENV['DATABASE_CREDENTIAL_PASSWORD']
  )
  res = @connection.exec("SELECT DISTINCT #{ENV['column_name']} as column_value, #{ENV['name_column'] || ENV['column_name']} as column_name FROM #{ENV['schema_and_table']}").values
  new_features = res.each_with_index.map {|res, index|
    current = {
      id: base_id + index,
      name: res.first,
      value: res.last
    }
    Hash.new.merge(feature_template).merge(current)
  }
  @json['other_seeds']['features'].push(*new_features)
  new_config = JSON.pretty_generate(@json)
  civis_json_id = post_url("https://#{ENV['CIVIS_API_KEY']}:@#{ENV['CIVIS_API_ENDPOINT']}/json_values", new_config)
  post_url("https://#{ENV['CIVIS_API_KEY']}:@#{ENV['CIVIS_API_ENDPOINT']}/scripts/containers/#{ENV['CIVIS_JOB_ID']}/runs/#{ENV['CIVIS_RUN_ID']}/outputs", {objectType: 'JSONValue', objectId: civis_json_id})
rescue PG::Error => e
  puts e.message
ensure
  @connection.close if @connection
end
