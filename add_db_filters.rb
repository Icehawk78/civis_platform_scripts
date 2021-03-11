require 'json'
require 'pg'
require 'rest-client'

begin
  @json = JSON.parse(RestClient.get(ENV['json_config_url']).body)
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
    host: ENV['DATABASE_HOST'],
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
  File.write('/tmp/config.json', new_config)
  upload_file = File.new('/tmp/config.json')
  civis_file = JSON.parse(RestClient.post("#{ENV['CIVIS_API_ENDPOINT']}/files", {name: 'config.json'}, {'Authorization': "Bearer #{ENV['CIVIS_API_KEY']}"}))
  upload_fields = civis_file['uploadFields']
  upload_fields['file'] = upload_file
  RestClient.post(civis_file['uploadUrl'] + '/', upload_fields, {'Authorization': "Bearer #{ENV['CIVIS_API_KEY']}"})
  # civis_file_id = post_url("#{ENV['CIVIS_API_ENDPOINT']}/json_values", {name: 'config.json', valueStr: new_config})
  RestClient.post("#{ENV['CIVIS_API_ENDPOINT']}/scripts/containers/#{ENV['CIVIS_JOB_ID']}/runs/#{ENV['CIVIS_RUN_ID']}/outputs", {objectType: 'File', objectId: civis_file['id']}, {'Authorization': "Bearer #{ENV['CIVIS_API_KEY']}"})
rescue PG::Error => e
  puts e.message
ensure
  @connection.close if @connection
end
