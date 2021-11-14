# Ruby #
input {
  file {
        path => "/home/user/jer43/**/*.txt"
	exit_after_read => "true"
	mode => "read"

  }
}

filter{
ruby {
        code => "
            event.set('datetime', [event.get('path').split('/')[-1].split('_')[0], event.get('path').split('/')[-1].split('_')[1]].join(' '))"
    }
date {
    	match => [ "datetime", "yyyyMMdd HHmm" ]
    }
	if "sonic" in [path]{
	ruby{
		code => 'event.set("[cols]", 1 + event.get("message").count(","))'
	}
	if [cols] == 10 {
		csv{
			add_tag => ["sonic","rotronics"]
			autogenerate_column_names => "false"
			columns => ['secondspasthour','d1','U','V','W','d2','speedofsound','sonictemperature','errorcode','checksum']		
		}
	}
	if [cols] == 14 {
		csv{
			add_tag => ["sonic"]
			autogenerate_column_names => "false"
			columns => ['secondspasthour','d1','U','V','W','d2','speedofsound','sonictemperature','errorcode','V1','V2','V3','V4','checksum']
		}

	ruby{
		code => 'event.set("[temperature]", (event.get("V3") - 2) * 20)'
	}
	ruby{
		code => 'event.set("[relativehum]", event.get("V4") * 100)'
	}
	}

}
	else if "logger" in [path]{
	
	ruby{
		code => 'event.set("[cols]", 1 + event.get("message").count(","))'
	}
	if [cols] == 12  {
		csv{
			add_tag => ["probe_a","logger"]
			autogenerate_column_names => "false"
			columns => ['secondspasthour','name','year','julianday','v1','v2','v3','v4','v5','soiltemp','soilmoisture','realdielectricpermittivity']
		}
}
	else if "MetData" in [message] {
	csv{
			add_tag => ["MetData","logger"]
			autogenerate_column_names => "false"
			columns => ['secondspasthour','tablename','batteryvoltage','paneltemperature','pressure','rainfall','temperature','relativehumidity','tempat10','tempat7','tempat4']
		}
}

else if "Solar" in [message] {
	csv{
			add_tag => ["Solar","logger"]
			autogenerate_column_names => "false"
			columns => ['secondspasthour','tablename','batteryvoltage','sup','sdn','lup','ldn','sensor_temp','sensor_temp','lwupcor','lwlowcor','albedo','rn','rsnet','rinet']
		}
}

else if "DataCold" in [message] {
	csv{
			add_tag => ["DataCold","logger"]
			autogenerate_column_names => "false"
			columns => ['secondspasthour','tablename','batteryvoltage','U_sen','U_heat']
		}
}

else if "DataWarm" in [message] {
	csv{
			add_tag => ["DataWarm","logger"]
			autogenerate_column_names => "false"
			columns => ['secondspasthour','tablename','U_sen0','U_senamp','thermalconductivity','E1','E1_Q']
		}
}


else if "DynaData" in [message] {
	csv{
			add_tag => ["DynaData","logger"]
			autogenerate_column_names => "false"
			columns => ['secondspasthour','tablename','thermaldiffusivity','volumetricheatcapacity']
		}
}

else if "HFP01SC_mean" in [message] {
	csv{
			add_tag => ["HFP01SC_mean","logger"]
			autogenerate_column_names => "false"
			columns => ['timestamp','tablename','soilheatflux','recalibratedheatflux']
		}
}
else if "Probe c" in [message] {
	csv{
			add_tag => ["Probe c","logger"]
			autogenerate_column_names => "false"
			columns => ['secondspasthour','name','year','julianday','time','V1','V2','V3','V4','V5','soiltemperature','soilmoisture','realdielectricpermittivity']
		}
}

else if [cols] == 7 {
	csv{
			add_tag => ["JER0036","logger"]
			autogenerate_column_names => "false"
			columns => ['secondspasthour','year','julianday','batteryvoltage','paneltemperature','barometricpressure','rainfall']
		}
}

else if [cols] == 12 {
	csv{
			add_tag => ["probea1","logger"]
			autogenerate_column_names => "false"
			columns => ['secondspasthour','name','year','julianday','v1','v2','v3','v4','v5','soiltemp','soilmoisture','realdielectricpermittivity']
		}
}

else if "Probe a" in [message] {
	csv{
			add_tag => ["probea1","logger"]
			autogenerate_column_names => "false"
			columns => ['secondspasthour','name','year','julianday','time','v1','v2','v3','v4','v5','soiltemp','soilmoisture','realdielectricpermittivity']
		}
}
else if [cols] == 13 {
	csv{
			add_tag => ["loggerdata","logger"]
			autogenerate_column_names => "false"
			columns => ['secondspasthour','year','julianday','time','batteryvoltage','paneltemp','barometricpressure','rainfall','tempat2','relativehumidity','tempat10','solarradiationwatt','solarradiationkilo']
		}
}

else {
	csv {}
}

}
}

output {
      elasticsearch {
        index => "dataanalysis"
      }
	stdout { codec => dots }
    }
