require 'json'
require 'pg'
require 'rest-client'

begin
  @json = JSON.parse(RestClient.get(ENV['json_config_url']).body)
  base_id = @json['other_seeds']['features'].map{|f| f['id']}.compact.max + 1
  root_feature = ENV['root_id'] ? @json['other_seeds']['features'].find{|f| f['id'] == ENV['root_id'].to_i} : nil
  if root_feature.nil?
    root_feature = {
      'id': base_id,
      'name': ENV['root_name'] || 'Default Root Name',
      'column_name': ENV['column_name'],
      'value': nil,
      'root_id': nil,
      'parent_id': nil,
      'ui_section_id': ENV['ui_section_id'].to_i,
      'variable_type_id': (ENV['is_multiselect'] ? 5 : 3),
      'is_cuttable': true,
      'is_plottable': true,
      'props': {}
    }
    @json['other_seeds']['features'] << root_feature
    base_id += 1
  end
  feature_template = {
    id: -1,
    name: '',
    column_name: '',
    value: '',
    root_id: root_feature['id'],
    parent_id: root_feature['id'],
    ui_section_id: root_feature['ui_section_id'],
    variable_type_id: root_feature['variable_type_id'],
    is_cuttable: root_feature['is_cuttable'],
    is_plottable: root_feature['is_plottable'],
    props: {}
  }

  @connection = PG::connect(
    host: ENV['DATABASE_HOST'],
    port: ENV['DATABASE_PORT'],
    dbname: ENV['DATABASE_DATABASE'],
    user: ENV['DATABASE_CREDENTIAL_USERNAME'],
    password: ENV['DATABASE_CREDENTIAL_PASSWORD']
  )
  value_column = ENV['column_name']
  name_column = (ENV['name_column'].nil? || ENV['name_column'].empty?) ? ENV['column_name'] : ENV['name_column']
  res = @connection.exec("SELECT DISTINCT #{value_column} as value_column, #{name_column} as name_column FROM #{ENV['schema_and_table']} WHERE #{value_column} IS NOT NULL AND #{name_column} IS NOT NULL ORDER BY 2 ASC").values
  new_features = res.each_with_index.map {|res, index|
    current = {
      id: base_id + index,
      name: res.last,
      column_name: ENV['column_name'],
      value: res.first,
      order: index
    }
    Hash.new.merge(feature_template).merge(current)
  }
  @json['other_seeds']['features'].push(*new_features)
  new_config = JSON.pretty_generate(@json)
  File.write('config.json', new_config)
  config_file = File.open('config.json', 'r')
  civis_file = JSON.parse(RestClient.post("#{ENV['CIVIS_API_ENDPOINT']}/files", {name: 'config.json'}, {'Authorization': "Bearer #{ENV['CIVIS_API_KEY']}"}))
  upload_fields = {key: civis_file['uploadFields']['key']}
  civis_file['uploadFields'].each{|k,v|
    upload_fields[k] = v unless k == 'key'
  }
  # upload_fields['multipart'] = true
  upload_fields['file'] = config_file
  puts 'Civis File:'
  puts civis_file
  puts 'Upload Fields Keys:'
  puts upload_fields
  RestClient.post(civis_file['uploadUrl'] + '/', upload_fields)
  # civis_file_id = post_url("#{ENV['CIVIS_API_ENDPOINT']}/json_values", {name: 'config.json', valueStr: new_config})
  RestClient.post("#{ENV['CIVIS_API_ENDPOINT']}/scripts/containers/#{ENV['CIVIS_JOB_ID']}/runs/#{ENV['CIVIS_RUN_ID']}/outputs", {objectType: 'File', objectId: civis_file['id']}, {'Authorization': "Bearer #{ENV['CIVIS_API_KEY']}"})
rescue => e
  puts e.message
ensure
  @connection.close if @connection
end
