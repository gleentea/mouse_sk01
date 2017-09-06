# coding: utf-8
require 'mqtt'
require 'json'
require 'pp'
require 'sqlite3'

db = SQLite3::Database.new('types.db')
client = MQTT::Client.connect('h60mqtt.cloudapp.net',1883)
client.subscribe('#')

types = {}
if File.exists? 'types.db.marshal'
  data = open('types.db.marshal').read
  types = Marshal.load data
  p "==== loaded types ===="
  pp types
  p "======================"  
end

client.get do |topic, message|
  next if message[0] != '{'
  json = JSON.parse(message)
  type = json['type']
  unless types.has_key?(type)
    puts "new json",topic, json
    case type
    when 20200 then
      # {"type"=>20200, "data"=>"{\"uuid\":\"54ab3a62b6ef\" ,\"sensorDataType\":\"Humidity\",\"sensorDataTypeSeq\":2,\"value\":50}"}
      data = JSON.parse(json['data'])
      uuid = data['uuid']
      types[type] ||= {}
      types[type][uuid] = json
    when 11200 then
      #{"type"=>11200, "data"=>"{  \"roomHubUUID\": \"54ab3a95934e\",  \"signal\": \"{  \\\"assetType\\\": 11,  \\\"brand\\\": \\\"EQL\\\",  \\\"brandId\\\": 0,  \\\"connectionType\\\": 1,  \\\"currentPower\\\": 252.53260803222659,  \\\"device\\\": \\\"PLUG\\\",  \\\"method\\\": 14,  \\\"modelId\\\": \\\"\\\",  \\\"onlineStatus\\\": 1,  \\\"power\\\": 1,  \\\"result\\\": 0,  \\\"startTime\\\": 1504666771000,  \\\"subtype\\\": 0,  \\\"timer\\\": 0,  \\\"totalPower\\\": 3068.773681640625,  \\\"type\\\": 14,  \\\"useTime\\\": 43878,  \\\"useTime1\\\": 43884,  \\\"uuid\\\": \\\"D4F51373FEEB\\\"}\"}"}
      data = JSON.parse(json['data'])
      signal = JSON.parse(data['signal'])
      uuid = data['roomHubUUID']
      sensor_uuid = signal['uuid']
      # types[type] ||= {}
      # types[type][uuid] ||= {}
      # types[type][uuid][sensor_uuid] = json
    when 10900 then
      # {"type"=>10900, "data"=>"{  \"roomHubUUID\": \"54ab3a573293\",  \"uuid\": \"E6AD9FE046EB\",  \"deviceName\": \"E6AD9FE046EB\",  \"brandId\": 0,  \"modelId\": \"00000000-0000-0000-0000-000000000000\",  \"brandName\": \"EQL\",  \"modelName\": \"PM2.5\",  \"assetType\": 3,  \"power\": 0,  \"property\": \"{\\r\\n  \\\"capacity\\\": 1,\\r\\n  \\\"adapter\\\": 1,\\r\\n  \\\"uuid\\\": \\\"E6AD9FE046EB\\\",\\r\\n  \\\"value\\\": 14.0\\r\\n}\",  \"createAt\": \"2017-09-06T02:47:03.503+00:00\",  \"subType\": 0}"}
      data = JSON.parse(json['data'])
      uuid = data['roomHubUUID']
      sensor_uuid = data['uuid']
      # types[type] ||= {}
      # types[type][uuid] ||= {}
      # types[type][uuid][sensor_uuid] = json
    when 10400 then
      # {"type"=>10400, "data"=>"{  \"id\": \"ea509084-5227-4d6b-87ce-98d101f3d64b\",  \"uuid\": \"54ab3a62a6d4\",  \"userId\": \"042e5ced-3665-4dbb-8148-431a2d95d612\",  \"status\": false,  \"onlineStatus\": false,  \"updateOnlineStatusTime\": 0,  \"deviceName\": \"30-1客廳\",  \"ip\": \"61.223.224.85\",  \"port\": 0,  \"deviceType\": 0,  \"subtype\": 0,  \"deviceModel\": null,  \"brandName\": null,  \"modelName\": null,  \"townId\": \"202a8369-e806-4cd1-bb79-23106a9ec7bc\",  \"roleName\": \"Administrator\",  \"favTemp\": 26.0,  \"ownerName\": \"mark line\",  \"shareCnt\": 0,  \"version\": \"1.1.13.17\",  \"btMac\": \"\",  \"roomHubUUID\": null,  \"deviceAssets\": [    {      \"roomHubUUID\": \"54ab3a62a6d4\",      \"uuid\": \"54ab3a62a6d40001\",      \"deviceName\": \"54ab3a62a6d40001\",      \"brandId\": 0,      \"modelId\": \"00000000-0000-0000-0000-000000000000\",      \"brandName\": \"DIGISTAR\",      \"modelName\": \"Remote code 421\",      \"assetType\": 0,      \"power\": 0,      \"property\": \"\",      \"createAt\": \"0001-01-01T00:00:00+00:00\",      \"subType\": 0    }  ]}"}
      data = JSON.parse(json['data'])
      uuid = data['uuid']
    when 20400 then
      # {"type"=>20400, "data"=>"{  \"uuid\": \"F0C77F8826D3\",  \"watt\": 0.89,  \"power\": 1,  \"timeStamp\": 0}"}
      data = JSON.parse(json['data'])
      uuid = data['uuid']
    when 30200 then
      # {"type"=>30200, "data"=>"{  \"deviceType\": 0,  \"className\": \"DeviceInfo\",  \"deviceSetting\": \"{  \\\"DeviceInfo\\\": {    \\\"WiFiBridge\\\": 0,    \\\"WifiApMac\\\": \\\"1CABC0213E58\\\",    \\\"assetUpdateTime\\\": \\\"03:00\\\",    \\\"daylightSaving\\\": 0,    \\\"timeZone\\\": 480,    \\\"timeZoneId\\\": \\\"\\\",    \\\"townId\\\": \\\"67794b04-62ca-4635-9256-43b419adf6e2\\\",    \\\"useDefaultAssetUpdateTime\\\": 0,    \\\"userId\\\": \\\"2ccfd1b9-3815-4199-aa1f-710575b5d6e6\\\"  }}\"}", "uuid"=>"54ab3a62a32c"}
      data = JSON.parse(json['data'])
      uuid = json['uuid']
    when 10500 then
    # {"type"=>10500, "data"=>"{  \"roomHubUUID\": \"54ab3a62a176\",  \"uuid\": \"54ab3a62a176\",  \"power\": 1,  \"temp\": 26,  \"mode\": 1,  \"swing\": 1,  \"fan\": 0,  \"timerOn\": -1,  \"timerOff\": 131094,  \"userId\": \"b889b8c4-20de-457a-9561-4f6265a21907\",  \"brand\": \"SAMPO 新寶\",  \"device\": \"AR-1639\",  \"subtype\": 0,  \"connectionType\": 0,  \"brandId\": 3283,  \"modelId\": \"8cb995a7-640e-4aeb-a7e9-9ab148b1d94a\"}"}
      data = JSON.parse(json['data'])
      uuid = data['roomHubUUID']
    when 20500 then
      # {"type"=>20500, "data"=>"{  \"uuid\": \"78A5047428A4\",  \"sensorDataType\": \"doorsensor\",  \"status\": 2,  \"timeStamp\": 1504718756490}"}
      data = JSON.parse(json['data'])
      uuid = data['uuid']
    else
      puts 'unknown type'
      puts topic, json
    end
    types[type] ||= {}
    types[type][uuid] ||= {}
    types[type][uuid] = json
    open('types.db.marshal','w').write(Marshal.dump(types))
  else

  end
end
